package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io/fs"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type NativeBackupConfig struct {
	Directory string
	S3        *DuckLakeS3Config
	Interval  time.Duration
	Timeout   time.Duration
	Retain    int
}

type NativeBackupStatus struct {
	Enabled          bool   `json:"enabled"`
	SourceID         string `json:"source_id,omitempty" doc:"Analytical backup source identity for offline restore; distinct from event source_id."`
	State            string `json:"state" enum:"waiting,running,ok,failed,interrupted"`
	IntervalSeconds  int64  `json:"interval_seconds"`
	TimeoutSeconds   int64  `json:"timeout_seconds"`
	RetainCompleted  int    `json:"retain_completed" doc:"Configured maximum number of completed bundles retained for this native source."`
	LastAttempt      string `json:"last_attempt,omitempty"`
	LastSuccess      string `json:"last_success,omitempty"`
	LastCheckpointAt string `json:"last_checkpoint_at,omitempty"`
	LatestBundle     string `json:"latest_bundle,omitempty"`
}

type nativeBackupRuntime struct {
	config       NativeBackupConfig
	databasePath string
	mu           sync.Mutex
	status       NativeBackupStatus
	slot         chan struct{}
	cancel       context.CancelFunc
	done         chan struct{}
}

func prepareNativeBackup(c *NativeBackupConfig, database string, lake *DuckLakeConfig) (*nativeBackupRuntime, error) {
	if c == nil {
		return nil, nil
	}
	if lake != nil && lake.S3 != nil && c.S3 == nil {
		return nil, errors.New("native local backups do not include remote analytical S3 objects; disable native backups for this storage mode")
	}
	if lake == nil || lake.Directory == "" || database == ":memory:" || (c.Directory == "" && c.S3 == nil) {
		return nil, errors.New("native backups require file-backed SQLite, DuckLake and one backup destination")
	}
	if c.Directory != "" && c.S3 != nil {
		return nil, errors.New("configure exactly one native backup destination")
	}
	config := *c
	if config.Interval == 0 {
		config.Interval = 24 * time.Hour
	}
	if config.Timeout == 0 {
		config.Timeout = 5 * time.Minute
	}
	if config.Retain == 0 {
		config.Retain = 7
	}
	if config.Interval < time.Minute || config.Timeout < time.Second || config.Timeout > time.Hour || config.Retain < 1 || config.Retain > 1000 {
		return nil, errors.New("invalid native backup schedule, timeout or retention")
	}
	path, err := filepath.Abs(database)
	if err != nil {
		return nil, err
	}
	if config.S3 != nil {
		normalized, err := normalizeDuckLakeS3(config.S3)
		if err != nil {
			return nil, errors.New("invalid native backup S3 configuration")
		}
		config.S3 = normalized
		source, err := normalizeDuckLakeS3(lake.S3)
		if err != nil {
			return nil, errors.New("invalid analytical S3 configuration for native backups")
		}
		if source != nil && source.Bucket == normalized.Bucket && (strings.HasPrefix(normalized.Prefix+"/", source.Prefix+"/") || strings.HasPrefix(source.Prefix+"/", normalized.Prefix+"/")) {
			return nil, errors.New("backup S3 destination must not overlap analytical storage")
		}
		return &nativeBackupRuntime{config: config, databasePath: path, slot: make(chan struct{}, 1), status: NativeBackupStatus{Enabled: true, State: "waiting", IntervalSeconds: int64(config.Interval / time.Second), TimeoutSeconds: int64(config.Timeout / time.Second), RetainCompleted: config.Retain}}, nil
	}
	directory, err := filepath.Abs(config.Directory)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}
	directory, err = filepath.EvalSymlinks(directory)
	if err != nil {
		return nil, err
	}
	native, err := filepath.Abs(lake.Directory)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(native, 0700); err != nil {
		return nil, err
	}
	native, err = filepath.EvalSymlinks(native)
	if err != nil {
		return nil, err
	}
	relative, err := filepath.Rel(native, directory)
	if err != nil || relative == "." || filepath.IsLocal(relative) {
		return nil, errors.New("backup destination must be outside the DuckLake directory")
	}
	if inverse, err := filepath.Rel(directory, native); err != nil || inverse == "." || filepath.IsLocal(inverse) {
		return nil, errors.New("backup destination must not contain the DuckLake directory")
	}
	databaseLocation := path
	if parent, err := filepath.EvalSymlinks(filepath.Dir(path)); err == nil {
		databaseLocation = filepath.Join(parent, filepath.Base(path))
	}
	if relative, err := filepath.Rel(directory, databaseLocation); err != nil || relative == "." || filepath.IsLocal(relative) {
		return nil, errors.New("backup destination must not contain the live SQLite database")
	}
	info, err := os.Stat(directory)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("backup directory must be private to its owner")
	}
	config.Directory = directory
	return &nativeBackupRuntime{config: config, databasePath: path, slot: make(chan struct{}, 1), status: NativeBackupStatus{Enabled: true, State: "waiting", IntervalSeconds: int64(config.Interval / time.Second), TimeoutSeconds: int64(config.Timeout / time.Second), RetainCompleted: config.Retain}}, nil
}

func (s *Service) startNativeBackups() error {
	runtime := s.backups
	if runtime == nil {
		return nil
	}
	var raw string
	err := s.db.QueryRow("SELECT value FROM metadata WHERE key='native_backup_status'").Scan(&raw)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if raw != "" {
		if err := json.Unmarshal([]byte(raw), &runtime.status); err != nil {
			return errors.New("invalid native backup status")
		}
		runtime.status.Enabled = true
		runtime.status.IntervalSeconds = int64(runtime.config.Interval / time.Second)
		runtime.status.TimeoutSeconds = int64(runtime.config.Timeout / time.Second)
		runtime.status.RetainCompleted = runtime.config.Retain
		if runtime.status.State == "running" {
			runtime.status.State = "interrupted"
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	runtime.cancel = cancel
	runtime.done = make(chan struct{})
	delay := time.Second
	if last, err := time.Parse(time.RFC3339Nano, runtime.status.LastSuccess); err == nil && runtime.status.State == "ok" {
		delay = max(time.Second, time.Until(last.Add(runtime.config.Interval)))
	}
	go func() {
		defer close(runtime.done)
		timer := time.NewTimer(delay)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			}
			attempt, stop := context.WithTimeout(ctx, runtime.config.Timeout)
			err := s.createNativeBackup(attempt)
			stop()
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				slog.Warn("native backup failed", "error", err)
				timer.Reset(min(time.Minute, runtime.config.Interval))
			} else {
				timer.Reset(runtime.config.Interval)
			}
		}
	}()
	return nil
}

func (s *Service) stopNativeBackups() {
	if s.backups != nil && s.backups.cancel != nil {
		s.backups.cancel()
		<-s.backups.done
	}
}

func (s *Service) nativeBackupStatus() *NativeBackupStatus {
	if s.backups == nil {
		return nil
	}
	s.backups.mu.Lock()
	defer s.backups.mu.Unlock()
	status := s.backups.status
	return &status
}

func (s *Service) recordNativeBackupStatus(ctx context.Context, state, bundle, captured string) error {
	runtime := s.backups
	var sourceID string
	if state == "running" {
		var err error
		sourceID, err = s.lake.SourceID(ctx)
		if err != nil {
			return err
		}
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.status.State = state
	if state == "running" {
		runtime.status.LastAttempt = now()
		runtime.status.SourceID = sourceID
	}
	if state == "ok" {
		runtime.status.LastSuccess = now()
		runtime.status.LastCheckpointAt = captured
		runtime.status.LatestBundle = bundle
	}
	encoded, err := json.Marshal(runtime.status)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, "INSERT INTO metadata(key,value) VALUES('native_backup_status',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", string(encoded))
	return err
}

func (s *Service) createNativeBackup(ctx context.Context) (result error) {
	runtime := s.backups
	ctx, cancel := context.WithTimeout(ctx, runtime.config.Timeout)
	defer cancel()
	select {
	case runtime.slot <- struct{}{}:
		defer func() { <-runtime.slot }()
	default:
		return errors.New("native backup already running")
	}
	defer func() {
		if result != nil {
			record, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			result = errors.Join(result, s.recordNativeBackupStatus(record, "failed", "", ""))
		}
	}()
	if err := s.recordNativeBackupStatus(ctx, "running", "", ""); err != nil {
		return err
	}
	writerID, err := s.nativeBackupWriterID(ctx)
	if err != nil {
		return err
	}
	if runtime.config.S3 != nil {
		return s.createNativeS3Backup(ctx, writerID)
	}
	sourceID, err := s.lake.SourceID(ctx)
	if err != nil {
		return err
	}
	unlockDirectory, err := lockProjectionFile(ctx, filepath.Join(runtime.config.Directory, ".porcupine-backup.lock"), true)
	if err != nil {
		return err
	}
	defer unlockDirectory()
	if err := claimLocalNativeBackupOwner(runtime.config.Directory, sourceID, writerID); err != nil {
		return err
	}
	if err := removeIncompleteNativeBackups(runtime.config.Directory); err != nil {
		return err
	}
	release, err := s.projectionGuard.exclusive(ctx)
	if err != nil {
		return err
	}
	var checkpoint *nativeCheckpoint
	var capturedAt string
	func() {
		defer release()
		checkpoint, err = s.lake.stageCheckpoint(ctx, func(ctx context.Context, destination string) error {
			// A separate source handle lets acceptance continue between backup steps.
			source, openErr := sql.Open("sqlite", (&url.URL{Scheme: "file", Path: runtime.databasePath}).String()+"?mode=ro&_pragma=busy_timeout(100)")
			if openErr != nil {
				return openErr
			}
			defer source.Close()
			err := snapshotSQLite(ctx, source, destination)
			if err == nil {
				capturedAt = now()
			}
			return err
		})
	}()
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, checkpoint.Close()) }()
	if err := validateNativeBackupStorageIdentity(checkpoint.Storage, false); err != nil {
		return err
	}
	stage := checkpoint.Directory
	// Native projection and cleanup can resume now: only immutable hardlinks
	// and copied catalog/SQLite files remain in the private staging directory.
	if sourceID != checkpoint.SourceID {
		return errors.New("native backup source identity changed during capture")
	}
	id := "pc_backup_" + newID()
	partial := filepath.Join(runtime.config.Directory, "."+id+".incomplete")
	if err := os.Mkdir(partial, 0700); err != nil {
		return err
	}
	defer os.RemoveAll(partial)
	previousDirectory := ""
	previousFiles := map[string]backupFile{}
	if previous := s.nativeBackupStatus().LatestBundle; filepath.IsLocal(previous) && filepath.Base(previous) == previous && strings.HasPrefix(previous, "pc_backup_") {
		candidate := filepath.Join(runtime.config.Directory, previous)
		if old, verifyErr := verifyNativeBackup(ctx, candidate); verifyErr == nil && old.SourceID == sourceID {
			previousDirectory = candidate
			for _, file := range old.Files {
				previousFiles[file.Path] = file
			}
		}
	}
	manifest := nativeBackupManifest{Format: "porcupine-native-v1", ID: id, Version: Version, SourceID: sourceID, CreatedAt: now(), CapturedAt: capturedAt, SQLitePath: "acceptance.sqlite", DuckLakeDirectory: "ducklake", Catalogs: checkpoint.Catalogs, SourceStorage: checkpoint.Storage}
	if err := filepath.WalkDir(stage, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		relative, err := filepath.Rel(stage, path)
		if err != nil {
			return err
		}
		target := filepath.Join(partial, relative)
		if entry.IsDir() {
			return os.MkdirAll(target, 0700)
		}
		if !entry.Type().IsRegular() {
			return errors.New("backup stage contains a non-regular file")
		}
		if len(manifest.Files) >= nativeBackupFileLimit {
			return errors.New("native backup file inventory limit reached; reduce the small-file backlog before retrying")
		}
		file, err := hashBackupFile(ctx, path, filepath.ToSlash(relative))
		if err != nil {
			return err
		}
		reused := false
		if previousDirectory != "" && strings.Contains(file.Path, "/files/") && strings.HasSuffix(file.Path, ".parquet") {
			if old, ok := previousFiles[file.Path]; ok && old.SHA256 == file.SHA256 && old.Bytes == file.Bytes {
				reused = os.Link(filepath.Join(previousDirectory, filepath.FromSlash(file.Path)), target) == nil
			}
		}
		if !reused {
			if err := copyCheckpointFile(ctx, path, target); err != nil {
				return err
			}
		}
		manifest.Files = append(manifest.Files, file)
		return nil
	}); err != nil {
		return err
	}
	for _, path := range []string{filepath.Join(partial, "acceptance.sqlite"), filepath.Join(partial, "ducklake", "registry.sqlite")} {
		if err := validateBackupSQLite(ctx, path); err != nil {
			return err
		}
	}
	if err := writeBackupManifest(partial, manifest); err != nil {
		return err
	}
	if _, err := verifyNativeBackup(ctx, partial); err != nil {
		return err
	}
	complete := filepath.Join(runtime.config.Directory, id)
	if err := os.Rename(partial, complete); err != nil {
		return err
	}
	if err := syncBackupDirectory(runtime.config.Directory); err != nil {
		return err
	}
	if err := s.recordNativeBackupStatus(ctx, "ok", id, capturedAt); err != nil {
		return err
	}
	return pruneNativeBackups(runtime.config.Directory, sourceID, runtime.config.Retain, id)
}
