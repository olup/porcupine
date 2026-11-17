package lake

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"net/url"
	"os"
	"strings"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const sharedNativeReaderHorizon = 8 * 24 * time.Hour

var errSharedNativeDeleted = errors.New("DuckLake project is deleted")

// ValidateSharedNativeSettings checks operator input before the durable source ID exists.
func ValidateSharedNativeSettings(c *DuckLakeConfig) error {
	if c.PostgresURL == "" && c.SharedSourceID == "" {
		if c.MetadataTimeoutSeconds != 0 {
			return errors.New("DuckLake metadata timeout requires shared PostgreSQL metadata")
		}
		return nil
	}
	u, err := url.Parse(c.PostgresURL)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.Host == "" || c.S3 == nil {
		return errors.New("shared DuckLake requires a PostgreSQL URL and analytical S3 storage")
	}
	if c.MetadataTimeoutSeconds == 0 {
		c.MetadataTimeoutSeconds = defaultDuckLakeMetadataTimeoutSeconds
	}
	if _, err := nativePostgresMetadataURL(c.PostgresURL, c.MetadataTimeoutSeconds); err != nil {
		return err
	}
	return nil
}

func ValidateSharedNativeConfig(c *DuckLakeConfig) error {
	if err := ValidateSharedNativeSettings(c); err != nil {
		return err
	}
	if c.PostgresURL != "" && (len(c.SharedSourceID) != 36 || !LakePrefixSegmentPattern.MatchString(c.SharedSourceID)) {
		return errors.New("shared DuckLake requires a stable source ID")
	}
	return nil
}

func SharedNativeSchema(id string) string { return "pc_lake_" + strings.ReplaceAll(id, "-", "") }
func SharedNativeCatalogID(source, project string) string {
	digest := sha256.Sum256([]byte(source + "\x00" + project))
	raw := hex.EncodeToString(digest[:16])
	return raw[:8] + "-" + raw[8:12] + "-" + raw[12:16] + "-" + raw[16:20] + "-" + raw[20:]
}

func (s *DuckLakeStore) ConfigureSharedPostgres(ctx context.Context, db *sql.DB) error {
	// Explicit temporary secrets avoid the connector's default lookup in the
	// disabled local_file secret store and keep credentials out of ATTACH text.
	uri, err := nativePostgresMetadataURL(s.Config.PostgresURL, s.Config.MetadataTimeoutSeconds)
	if err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, "CREATE SECRET pc_analytics_postgres (TYPE postgres, URI "+LakeLiteral(uri)+")"); err != nil {
		return errors.New("shared analytical PostgreSQL credentials unavailable")
	}
	return nil
}

func (s *DuckLakeStore) initializeSharedNative(ctx context.Context) error {
	var actual string
	if err := s.Registry.QueryRowContext(ctx, "SELECT id FROM source").Scan(&actual); err != nil || actual != s.Config.SharedSourceID {
		return errors.New("shared DuckLake source identity changed")
	}
	db, err := sql.Open("pgx", s.Config.PostgresURL)
	if err != nil {
		return errors.New("shared analytical registry unavailable")
	}
	s.SharedCatalog = db
	db.SetMaxOpenConns(s.Config.MaxOpenProjects + 16)
	bounded, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	finish, err := s.sharedNativeInitialization(bounded, "__registry__")
	if err != nil {
		return err
	}
	defer finish()
	_, err = db.ExecContext(bounded, `CREATE TABLE IF NOT EXISTS pc_native_sources(source_id text PRIMARY KEY,fingerprint text NOT NULL);
 CREATE TABLE IF NOT EXISTS pc_native_projects(source_id text NOT NULL REFERENCES pc_native_sources(source_id),project_id text NOT NULL,catalog_id text NOT NULL,ready integer NOT NULL DEFAULT 0,deleted integer NOT NULL DEFAULT 0,PRIMARY KEY(source_id,project_id));`)
	if err != nil {
		return errors.New("shared analytical registry unavailable")
	}
	c := s.Config.S3
	digest := sha256.Sum256([]byte(c.Bucket + "\x00" + c.Prefix + "\x00" + c.Endpoint + "\x00" + c.Region))
	fingerprint := hex.EncodeToString(digest[:])
	if _, err := db.ExecContext(bounded, "INSERT INTO pc_native_sources(source_id,fingerprint) VALUES($1,$2) ON CONFLICT DO NOTHING", actual, fingerprint); err != nil {
		return errors.New("shared analytical source unavailable")
	}
	var stored string
	if err := db.QueryRowContext(bounded, "SELECT fingerprint FROM pc_native_sources WHERE source_id=$1", actual).Scan(&stored); err != nil || stored != fingerprint {
		return errors.New("shared analytical storage configuration changed")
	}
	return nil
}

func (s *DuckLakeStore) sharedNativeProject(ctx context.Context, project string) (string, bool, error) {
	id := SharedNativeCatalogID(s.Config.SharedSourceID, project)
	if _, err := s.SharedCatalog.ExecContext(ctx, "INSERT INTO pc_native_projects(source_id,project_id,catalog_id) VALUES($1,$2,$3) ON CONFLICT DO NOTHING", s.Config.SharedSourceID, project, id); err != nil {
		return "", false, errors.New("shared analytical registry unavailable")
	}
	var actual string
	var ready, deleted int
	if err := s.SharedCatalog.QueryRowContext(ctx, "SELECT catalog_id,ready,deleted FROM pc_native_projects WHERE source_id=$1 AND project_id=$2", s.Config.SharedSourceID, project).Scan(&actual, &ready, &deleted); err != nil {
		return "", false, errors.New("shared analytical registry unavailable")
	}
	if actual != id {
		return "", false, errors.New("shared analytical catalog identity changed")
	}
	if deleted != 0 {
		s.Registry.ExecContext(ctx, "DELETE FROM catalogs WHERE project_id=?", project)
		return "", false, errSharedNativeDeleted
	}
	if _, err := s.Registry.ExecContext(ctx, "INSERT INTO catalogs(project_id,id,ready) VALUES(?,?,?) ON CONFLICT(project_id) DO UPDATE SET id=excluded.id,ready=excluded.ready", project, id, ready); err != nil {
		return "", false, err
	}
	return actual, ready == 1, nil
}

// The SQL lock serializes schema creation. Projection correctness relies on the
// guard row written inside each native transaction, not this connection's life.
func (s *DuckLakeStore) sharedNativeInitialization(ctx context.Context, project string) (func(), error) {
	conn, err := s.SharedCatalog.Conn(ctx)
	if err != nil {
		return nil, errors.New("shared analytical registry unavailable")
	}
	lockName := s.Config.SharedSourceID + "\x00" + project
	if project == "__registry__" {
		lockName = "porcupine-native-registry-v1"
	}
	digest := sha256.Sum256([]byte(lockName))
	key := int64(binary.BigEndian.Uint64(digest[:8]))
	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", key); err != nil {
		conn.Close()
		return nil, errors.New("shared analytical initialization unavailable")
	}
	return func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if _, err := conn.ExecContext(cleanup, "SELECT pg_advisory_unlock($1)", key); err != nil {
			conn.Raw(func(any) error { return driver.ErrBadConn })
		}
		conn.Close()
	}, nil
}

func (p *ProjectLake) acquireNativeWrite(ctx context.Context, tx *sql.Tx) error {
	if !p.shared {
		return nil
	}
	var deleted bool
	if err := tx.QueryRowContext(ctx, "SELECT deleted FROM lake.project_write_guard WHERE id=1").Scan(&deleted); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errSharedNativeDeleted
		}
		return err
	}
	if deleted {
		return errSharedNativeDeleted
	}
	_, err := tx.ExecContext(ctx, "UPDATE lake.project_write_guard SET generation=generation+1 WHERE id=1")
	return err
}

func (s *DuckLakeStore) deleteSharedNativeProject(ctx context.Context, project string) error {
	finish, err := s.sharedNativeInitialization(ctx, project)
	if err != nil {
		return err
	}
	defer finish()
	if err := s.Mu.Lock(ctx); err != nil {
		return err
	}
	busy := s.opening[project] != nil
	if p := s.Projects[project]; p != nil && len(p.leases) != 0 {
		busy = true
	}
	s.Mu.Unlock()
	if busy {
		return errors.New("DuckLake project is in use")
	}
	var deleted, ready int
	var catalogID string
	err = s.SharedCatalog.QueryRowContext(ctx, "SELECT catalog_id,deleted,ready FROM pc_native_projects WHERE source_id=$1 AND project_id=$2", s.Config.SharedSourceID, project).Scan(&catalogID, &deleted, &ready)
	if errors.Is(err, sql.ErrNoRows) {
		catalogID = SharedNativeCatalogID(s.Config.SharedSourceID, project)
		_, err = s.SharedCatalog.ExecContext(ctx, "INSERT INTO pc_native_projects(source_id,project_id,catalog_id,deleted) VALUES($1,$2,$3,1)", s.Config.SharedSourceID, project, catalogID)
		if err != nil {
			return errors.New("shared analytical deletion unavailable")
		}
		deleted = 1
	} else if err != nil {
		return errors.New("shared analytical deletion unavailable")
	}
	if deleted == 0 && ready == 1 {
		// Open under the already-held initialization lock without recursively
		// acquiring it. The durable registry fence is written after the native guard.
		if err := s.Mu.Lock(ctx); err != nil {
			return err
		}
		p := s.Projects[project]
		s.Mu.Unlock()
		if p == nil {
			// A standalone cleanup engine skips registry initialization lock acquisition.
			p, err = s.openSharedDeletionProject(ctx, project, catalogID)
			if err != nil {
				return err
			}
			defer p.DB.Close()
		}
		err = p.markSharedDeleted(ctx)
		if err != nil {
			return err
		}
	}
	if _, err = s.SharedCatalog.ExecContext(ctx, "UPDATE pc_native_projects SET deleted=1 WHERE source_id=$1 AND project_id=$2", s.Config.SharedSourceID, project); err != nil {
		return errors.New("shared analytical deletion unavailable")
	}
	if err := s.Remote.deleteProject(ctx, project); err != nil {
		return err
	}
	// The catalog tombstone remains authoritative after physical metadata removal.
	if _, err = s.SharedCatalog.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+SharedNativeSchema(catalogID)+" CASCADE"); err != nil {
		return errors.New("shared analytical metadata cleanup unavailable")
	}
	if err = s.Mu.Lock(ctx); err != nil {
		return err
	}
	defer s.Mu.Unlock()
	if p := s.Projects[project]; p != nil && len(p.leases) == 0 {
		p.DB.Close()
		delete(s.Projects, project)
	}
	if _, err = s.Registry.ExecContext(ctx, "DELETE FROM catalogs WHERE project_id=?", project); err != nil {
		return err
	}
	return os.RemoveAll(s.ProjectDirectory(project))
}

func (p *ProjectLake) markSharedDeleted(ctx context.Context) error {
	return retrySharedNative(ctx, func() error {
		tx, err := p.DB.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if _, err = tx.ExecContext(ctx, "UPDATE lake.project_write_guard SET generation=generation+1,deleted=true WHERE id=1"); err != nil {
			return err
		}
		return tx.Commit()
	})
}

func (s *DuckLakeStore) openSharedDeletionProject(ctx context.Context, project, catalogID string) (*ProjectLake, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, err
	}
	fail := func() (*ProjectLake, error) {
		db.Close()
		return nil, errors.New("shared analytical deletion engine unavailable")
	}
	if _, err = db.ExecContext(ctx, "LOAD ducklake; LOAD postgres"); err != nil {
		return fail()
	}
	if _, err = db.ExecContext(ctx, "SET allow_persistent_secrets=false"); err != nil {
		return fail()
	}
	if err = s.ConfigureSharedPostgres(ctx, db); err != nil {
		return fail()
	}
	if _, err = s.Remote.Configure(ctx, db, project); err != nil {
		return fail()
	}
	attach := "ATTACH 'ducklake:postgres:' AS lake (DATA_PATH " + LakeLiteral(s.Remote.projectPath(project)) + ", METADATA_SCHEMA " + LakeLiteral(SharedNativeSchema(catalogID)) + ", META_SECRET 'pc_analytics_postgres', DATA_INLINING_ROW_LIMIT 0, CREATE_IF_NOT_EXISTS false, OVERRIDE_DATA_PATH false)"
	if _, err = db.ExecContext(ctx, attach); err != nil {
		return fail()
	}
	return &ProjectLake{DB: db, shared: true}, nil
}

func retrySharedNative(ctx context.Context, operation func() error) error {
	for attempt := 0; ; attempt++ {
		err := operation()
		if err == nil || attempt >= 15 || !isSharedNativeConflict(err) {
			return err
		}
		timer := time.NewTimer(time.Duration(min(5*(attempt+1), 100)) * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func isSharedNativeConflict(err error) bool {
	// Bound diagnostics to native transaction errors; connection/authentication
	// errors must never be retried based on server-provided strings.
	var native *duckdb.Error
	return errors.As(err, &native) && native.Type == duckdb.ErrorTypeTransaction
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
