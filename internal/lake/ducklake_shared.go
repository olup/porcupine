package lake

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

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

func (s *DuckLakeStore) sharedNativeAttach(dataPath, catalogID string, create bool) string {
	return fmt.Sprintf(`ATTACH 'ducklake:postgres:' AS lake (DATA_PATH %s, METADATA_SCHEMA %s,
META_SECRET 'pc_analytics_postgres', DATA_INLINING_ROW_LIMIT 0, OVERRIDE_DATA_PATH false, CREATE_IF_NOT_EXISTS %t, ENCRYPTED %t)`,
		LakeLiteral(dataPath), LakeLiteral(SharedNativeSchema(catalogID)), create, s.Config.Encrypted)
}

func (s *DuckLakeStore) initializeSharedNative(ctx context.Context) error {
	actual := s.Config.SharedSourceID
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
	if err := migrateNativeRegistry(bounded, db); err != nil {
		return fmt.Errorf("shared analytical registry migration: %w", err)
	}
	c := s.Config.S3
	digest := sha256.Sum256([]byte(c.Bucket + "\x00" + c.Prefix + "\x00" + c.Endpoint + "\x00" + c.Region))
	fingerprint := hex.EncodeToString(digest[:])
	if _, err := db.ExecContext(bounded, "INSERT INTO pc_native_sources(source_id,fingerprint,encrypted) VALUES($1,$2,$3) ON CONFLICT DO NOTHING", actual, fingerprint, s.Config.Encrypted); err != nil {
		return errors.New("shared analytical source unavailable")
	}
	var stored string
	var encrypted bool
	if err := db.QueryRowContext(bounded, "SELECT fingerprint,encrypted FROM pc_native_sources WHERE source_id=$1", actual).Scan(&stored, &encrypted); err != nil || stored != fingerprint {
		return errors.New("shared analytical storage configuration changed")
	}
	if encrypted != s.Config.Encrypted {
		return errors.New(duckLakeEncryptionModeChanged)
	}
	s.registry = &postgresNativeRegistry{db: db, source: actual, fingerprint: fingerprint, timeout: time.Duration(s.Config.MetadataTimeoutSeconds) * time.Second}
	return nil
}

func (s *DuckLakeStore) sharedNativeProject(ctx context.Context, project string) (string, bool, error) {
	record, err := s.registry.ensure(ctx, project)
	if err != nil {
		return "", false, errors.New("shared analytical registry unavailable")
	}
	if record.Deleted {
		return "", false, errSharedNativeDeleted
	}
	return record.CatalogID, record.Ready, nil
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

// Legacy mutation/import paths have no hosted epoch or durable effect outcome.
// They cannot bypass hosted sealing by updating only the old project guard.
func (p *ProjectLake) acquireNativeWrite(ctx context.Context, tx *sql.Tx) error {
	if p.shared {
		if err := rejectHostedLegacyOperation(ctx, tx); err != nil {
			return err
		}
	}
	return p.acquireNativeProjectGuard(ctx, tx)
}

func (p *ProjectLake) acquireNativeProjectGuard(ctx context.Context, tx *sql.Tx) error {
	if !p.shared {
		return nil
	}
	if err := checkNativeProjectNotDeleted(ctx, tx); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, "UPDATE lake.project_write_guard SET generation=generation+1 WHERE id=1")
	return err
}

// After the drained hosted cutover, legacy writers and deletion are rejected.
// Hosted writes conflict on hosted_write_guard and only read the legacy guard.
func checkNativeProjectNotDeleted(ctx context.Context, tx *sql.Tx) error {
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
	return nil
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
	var hostedDeletion bool
	if err := s.SharedCatalog.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pc_native_hosted_deletion WHERE source_id=$1 AND project_id=$2)`, s.Config.SharedSourceID, project).Scan(&hostedDeletion); err != nil {
		return errors.New("shared analytical deletion unavailable")
	}
	if hostedDeletion {
		return ErrHostedNativeGuardRequired
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
	return os.RemoveAll(s.ProjectDirectory(project))
}

func (p *ProjectLake) markSharedDeleted(ctx context.Context) error {
	if err := rejectHostedLegacyOperation(ctx, p.DB); err != nil {
		return err
	}
	return retryNativeTransaction(ctx, func() error {
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
	if _, err = db.ExecContext(ctx, s.sharedNativeAttach(s.Remote.projectPath(project), catalogID, false)); err != nil {
		return fail()
	}
	return &ProjectLake{DB: db, shared: true}, nil
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
