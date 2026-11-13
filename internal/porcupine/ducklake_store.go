//go:build ducklake

package porcupine

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

type duckLakeStore struct {
	root         string
	config       DuckLakeConfig
	registry     *sql.DB
	registryInfo os.FileInfo
	owner        *os.File
	mu           contextMutex
	projects     map[string]*projectLake
	closed       bool
}

type projectLake struct {
	db          *sql.DB
	write       contextMutex
	maintenance contextMutex
	directory   string
	catalogInfo os.FileInfo
	leases      int
	lastUsed    time.Time
}

func newDuckLake(config DuckLakeConfig) (lakeStore, error) {
	if config.Directory == "" {
		return nil, errors.New("DuckLake directory is required")
	}
	if config.MemoryLimitMB == 0 {
		config.MemoryLimitMB = 256
	}
	if config.Threads == 0 {
		config.Threads = 2
	}
	if config.MaxOpenProjects == 0 {
		config.MaxOpenProjects = 4
	}
	if config.MemoryLimitMB < 64 || config.MemoryLimitMB > 32768 || config.Threads < 1 || config.Threads > 4 || config.MaxOpenProjects < 1 || config.MaxOpenProjects > 64 {
		return nil, errors.New("invalid DuckLake resource configuration")
	}
	root, err := filepath.Abs(config.Directory)
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return nil, err
	}
	if !projectionLockSupported() {
		return nil, errors.New("DuckLake requires supported local file locking")
	}
	owner, err := os.OpenFile(filepath.Join(root, ".writer.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = projectionFileLock(owner, true); err != nil {
		owner.Close()
		return nil, errors.New("DuckLake directory already has a writer")
	}
	registryPath := filepath.Join(root, "registry.sqlite")
	registry, err := sql.Open("sqlite", registryPath+"?_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=busy_timeout(5000)")
	if err != nil {
		owner.Close()
		return nil, err
	}
	registry.SetMaxOpenConns(1)
	s := &duckLakeStore{root: root, config: config, registry: registry, owner: owner, projects: make(map[string]*projectLake)}
	fail := func(err error) (lakeStore, error) { registry.Close(); owner.Close(); return nil, err }
	if _, err = registry.Exec(`CREATE TABLE IF NOT EXISTS source (id TEXT PRIMARY KEY); CREATE TABLE IF NOT EXISTS catalogs (project_id TEXT PRIMARY KEY, id TEXT NOT NULL, ready INTEGER NOT NULL DEFAULT 0);`); err != nil {
		return fail(err)
	}
	var count int
	if err = registry.QueryRow("SELECT count(*) FROM source").Scan(&count); err != nil || count > 1 {
		return fail(errors.New("invalid DuckLake source registry"))
	}
	if count == 0 {
		if _, err = registry.Exec("INSERT INTO source(id) VALUES(?)", newID()); err != nil {
			return fail(err)
		}
	}
	if err = os.Chmod(registryPath, 0600); err != nil {
		return fail(err)
	}
	s.registryInfo, err = os.Stat(registryPath)
	if err != nil {
		return fail(err)
	}
	return s, nil
}

func (s *duckLakeStore) SourceID(ctx context.Context) (string, error) {
	if err := s.mu.Lock(ctx); err != nil {
		return "", err
	}
	defer s.mu.Unlock()
	return s.sourceIDLocked(ctx)
}

func (s *duckLakeStore) sourceIDLocked(ctx context.Context) (string, error) {
	if s.closed {
		return "", errors.New("DuckLake is closed")
	}
	info, err := os.Stat(filepath.Join(s.root, "registry.sqlite"))
	if err != nil || !os.SameFile(info, s.registryInfo) {
		return "", errors.New("DuckLake source registry changed")
	}
	var id string
	if err := s.registry.QueryRowContext(ctx, "SELECT id FROM source").Scan(&id); err != nil || id == "" {
		return "", errors.New("DuckLake source unavailable")
	}
	return id, nil
}

func (s *duckLakeStore) projectDirectory(project string) string {
	digest := sha256.Sum256([]byte(project))
	return filepath.Join(s.root, hex.EncodeToString(digest[:]))
}

func (s *duckLakeStore) leaseProject(ctx context.Context, project string) (*projectLake, func(), error) {
	if project == "" || len(project) > 1000 {
		return nil, nil, errors.New("invalid DuckLake project")
	}
	if err := s.mu.Lock(ctx); err != nil {
		return nil, nil, err
	}
	defer s.mu.Unlock()
	if s.closed {
		return nil, nil, errors.New("DuckLake is closed")
	}
	if _, err := s.sourceIDLocked(ctx); err != nil {
		return nil, nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	p := s.projects[project]
	if p != nil {
		info, err := os.Stat(filepath.Join(p.directory, "catalog.ducklake"))
		if err != nil || !os.SameFile(info, p.catalogInfo) {
			return nil, nil, errors.New("DuckLake project catalog changed")
		}
	} else {
		if len(s.projects) >= s.config.MaxOpenProjects {
			var oldest string
			for id, candidate := range s.projects {
				if candidate.leases == 0 && (oldest == "" || candidate.lastUsed.Before(s.projects[oldest].lastUsed)) {
					oldest = id
				}
			}
			if oldest == "" {
				return nil, nil, errors.New("DuckLake active project limit reached")
			}
			if err := s.projects[oldest].db.Close(); err != nil {
				return nil, nil, err
			}
			delete(s.projects, oldest)
		}
		var err error
		p, err = s.openProject(ctx, project)
		if err != nil {
			return nil, nil, err
		}
		s.projects[project] = p
	}
	p.leases++
	return p, func() {
		_ = s.mu.Lock(context.Background())
		p.leases--
		p.lastUsed = time.Now()
		s.mu.Unlock()
	}, nil
}

func lakeLiteral(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func (s *duckLakeStore) openProject(ctx context.Context, project string) (*projectLake, error) {
	directory := s.projectDirectory(project)
	var catalogID string
	var ready bool
	err := s.registry.QueryRowContext(ctx, "SELECT id,ready FROM catalogs WHERE project_id=?", project).Scan(&catalogID, &ready)
	if errors.Is(err, sql.ErrNoRows) {
		catalogID = newID()
		if _, err = s.registry.ExecContext(ctx, "INSERT INTO catalogs(project_id,id) VALUES(?,?)", project, catalogID); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	catalog := filepath.Join(directory, "catalog.ducklake")
	if ready {
		if _, err := os.Stat(catalog); err != nil {
			return nil, errors.New("DuckLake project catalog is missing")
		}
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(4)
	fail := func(err error) (*projectLake, error) { db.Close(); return nil, err }
	if _, err = db.ExecContext(ctx, "LOAD ducklake"); err != nil {
		if _, err = db.ExecContext(ctx, "INSTALL ducklake; LOAD ducklake"); err != nil {
			return fail(errors.New("DuckLake extension is unavailable"))
		}
	}
	for _, statement := range []string{
		fmt.Sprintf("SET memory_limit='%dMiB'", s.config.MemoryLimitMB),
		fmt.Sprintf("SET threads=%d", s.config.Threads),
		"SET TimeZone='UTC'",
		"SET temp_directory=" + lakeLiteral(filepath.Join(directory, "temp")),
		"SET max_temp_directory_size='256MiB'",
		"ATTACH " + lakeLiteral("ducklake:"+catalog) + " AS lake (DATA_PATH " + lakeLiteral(filepath.Join(directory, "files")) + ")",
	} {
		if _, err = db.ExecContext(ctx, statement); err != nil {
			return fail(err)
		}
	}
	if ready {
		var actual string
		if err = db.QueryRowContext(ctx, "SELECT id FROM lake.catalog_identity").Scan(&actual); err != nil || actual != catalogID {
			return fail(errors.New("DuckLake project catalog identity changed"))
		}
	} else {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fail(err)
		}
		if _, err = tx.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS lake.catalog_identity(id VARCHAR)"); err != nil {
			tx.Rollback()
			return fail(err)
		}
		var actual string
		err = tx.QueryRowContext(ctx, "SELECT id FROM lake.catalog_identity").Scan(&actual)
		if errors.Is(err, sql.ErrNoRows) {
			_, err = tx.ExecContext(ctx, "INSERT INTO lake.catalog_identity VALUES(?)", catalogID)
		} else if err == nil && actual != catalogID {
			err = errors.New("DuckLake project catalog identity changed")
		}
		if err != nil {
			tx.Rollback()
			return fail(err)
		}
		if err = tx.Commit(); err != nil {
			return fail(err)
		}
	}
	if _, err = db.ExecContext(ctx, duckLakeSchema); err != nil {
		return fail(err)
	}
	if _, err = db.ExecContext(ctx, "CALL ducklake_set_option('lake','target_file_size','8MB')"); err != nil {
		return fail(err)
	}
	for _, statement := range []string{
		"SET allowed_directories=[" + lakeLiteral(directory+string(os.PathSeparator)) + "]",
		"SET enable_external_access=false",
		"SET autoinstall_known_extensions=false",
		"SET autoload_known_extensions=false",
		"SET lock_configuration=true",
	} {
		if _, err = db.ExecContext(ctx, statement); err != nil {
			return fail(err)
		}
	}
	if _, err = s.registry.ExecContext(ctx, "UPDATE catalogs SET ready=1 WHERE project_id=?", project); err != nil {
		return fail(err)
	}
	info, err := os.Stat(catalog)
	if err != nil {
		return fail(err)
	}
	return &projectLake{db: db, directory: directory, catalogInfo: info, lastUsed: time.Now()}, nil
}

func (s *duckLakeStore) DeleteProject(ctx context.Context, project string) error {
	if err := s.mu.Lock(ctx); err != nil {
		return err
	}
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("DuckLake is closed")
	}
	if _, err := s.sourceIDLocked(ctx); err != nil {
		return err
	}
	if p := s.projects[project]; p != nil {
		if p.leases != 0 {
			return errors.New("DuckLake project is in use")
		}
		if err := p.db.Close(); err != nil {
			return err
		}
		delete(s.projects, project)
	}
	if err := os.RemoveAll(s.projectDirectory(project)); err != nil {
		return err
	}
	_, err := s.registry.ExecContext(ctx, "DELETE FROM catalogs WHERE project_id=?", project)
	return err
}

func (s *duckLakeStore) Maintain(ctx context.Context) error {
	_, err := s.maintainSized(ctx, 1, false, 1<<20)
	return err
}

type duckLakeMaintenanceStats struct {
	Serialized     bool    `json:"serialized"`
	LockWaitMS     float64 `json:"lock_wait_ms"`
	LockHeldMS     float64 `json:"lock_held_ms"`
	FlushMS        float64 `json:"flush_ms"`
	CompactMS      float64 `json:"compact_ms"`
	RowsFlushed    int64   `json:"rows_flushed"`
	FilesProcessed int64   `json:"files_processed"`
	FilesCreated   int64   `json:"files_created"`
	Completed      bool    `json:"completed"`
}

func (s *duckLakeStore) maintainMeasured(ctx context.Context, maxCompactions int) ([]duckLakeMaintenanceStats, error) {
	return s.maintainPolicy(ctx, maxCompactions, true)
}

func (s *duckLakeStore) maintainPolicy(ctx context.Context, maxCompactions int, serialized bool) ([]duckLakeMaintenanceStats, error) {
	return s.maintainSized(ctx, maxCompactions, serialized, 0)
}

func (s *duckLakeStore) maintainSized(ctx context.Context, maxCompactions int, serialized bool, maxFileSize int64) ([]duckLakeMaintenanceStats, error) {
	if err := s.mu.Lock(ctx); err != nil {
		return nil, err
	}
	if s.closed {
		s.mu.Unlock()
		return nil, errors.New("DuckLake is closed")
	}
	if _, err := s.sourceIDLocked(ctx); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	projects := make([]*projectLake, 0, len(s.projects))
	for _, p := range s.projects {
		info, err := os.Stat(filepath.Join(p.directory, "catalog.ducklake"))
		if err != nil || !os.SameFile(info, p.catalogInfo) {
			s.mu.Unlock()
			return nil, errors.New("DuckLake project catalog changed")
		}
		projects = append(projects, p)
	}
	for _, p := range projects {
		p.leases++
	}
	s.mu.Unlock()
	defer func() {
		_ = s.mu.Lock(context.Background())
		defer s.mu.Unlock()
		for _, p := range projects {
			p.leases--
			p.lastUsed = time.Now()
		}
	}()
	var measurements []duckLakeMaintenanceStats
	for _, p := range projects {
		stats, err := p.maintainPolicy(ctx, maxCompactions, serialized, maxFileSize)
		measurements = append(measurements, stats)
		if err != nil {
			return measurements, err
		}
	}
	return measurements, nil
}

func (p *projectLake) maintainPolicy(ctx context.Context, maxCompactions int, serialized bool, maxFileSize int64) (stats duckLakeMaintenanceStats, err error) {
	stats.Serialized = serialized
	if err = p.maintenance.Lock(ctx); err != nil {
		return stats, err
	}
	defer p.maintenance.Unlock()
	start := time.Now()
	if serialized {
		if err = p.write.Lock(ctx); err != nil {
			stats.LockWaitMS = float64(time.Since(start)) / float64(time.Millisecond)
			return stats, err
		}
		stats.LockWaitMS = float64(time.Since(start)) / float64(time.Millisecond)
		held := time.Now()
		defer func() { stats.LockHeldMS = float64(time.Since(held)) / float64(time.Millisecond); p.write.Unlock() }()
	}
	start = time.Now()
	err = p.maintenanceCommand(ctx, "CALL ducklake_flush_inlined_data('lake')", &stats)
	stats.FlushMS = float64(time.Since(start)) / float64(time.Millisecond)
	if err != nil {
		return stats, err
	}
	start = time.Now()
	sizeFilter := ""
	if maxFileSize > 0 {
		sizeFilter = fmt.Sprintf(", max_file_size => %d", maxFileSize)
	}
	err = p.maintenanceCommand(ctx, fmt.Sprintf("CALL ducklake_merge_adjacent_files('lake', max_compacted_files => %d%s)", maxCompactions, sizeFilter), &stats)
	stats.CompactMS = float64(time.Since(start)) / float64(time.Millisecond)
	stats.Completed = err == nil
	return stats, err
}

func (p *projectLake) maintenanceCommand(ctx context.Context, statement string, stats *duckLakeMaintenanceStats) error {
	rows, err := p.db.QueryContext(ctx, statement)
	if err != nil {
		return err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	values, pointers := make([]any, len(columns)), make([]any, len(columns))
	for i := range values {
		pointers[i] = &values[i]
	}
	for rows.Next() {
		if err := rows.Scan(pointers...); err != nil {
			return err
		}
		for i, name := range columns {
			var counter *int64
			switch name {
			case "rows_flushed":
				counter = &stats.RowsFlushed
			case "files_processed":
				counter = &stats.FilesProcessed
			case "files_created":
				counter = &stats.FilesCreated
			}
			if counter == nil || values[i] == nil {
				continue
			}
			n, err := strconv.ParseInt(fmt.Sprint(values[i]), 10, 64)
			if err != nil {
				return errors.New("invalid DuckLake maintenance counters")
			}
			*counter += n
		}
	}
	return rows.Err()
}

func (s *duckLakeStore) Close() error {
	_ = s.mu.Lock(context.Background())
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	for _, p := range s.projects {
		if p.leases != 0 {
			return errors.New("DuckLake is in use")
		}
	}
	s.closed = true
	var err error
	for _, p := range s.projects {
		err = errors.Join(err, p.db.Close())
	}
	err = errors.Join(err, s.registry.Close(), s.owner.Close())
	return err
}
