package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

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
	config              lakeapi.NativeBackupConfig
	databasePath        string
	controlDatabasePath string
	mu                  sync.Mutex
	status              NativeBackupStatus
	slot                chan struct{}
	cancel              context.CancelFunc
	done                chan struct{}
}

func prepareNativeBackup(c *lakeapi.NativeBackupConfig, database, controlDatabase string, lake *lakeapi.DuckLakeConfig) (*nativeBackupRuntime, error) {
	if c == nil {
		return nil, nil
	}
	config, err := normalizeNativeBackupConfig(c, database, controlDatabase, lake)
	if err != nil {
		return nil, err
	}
	path, err := filepath.Abs(database)
	if err != nil {
		return nil, err
	}
	controlPath, err := filepath.Abs(controlDatabase)
	if err != nil {
		return nil, err
	}
	if path == controlPath {
		return nil, errors.New("native backup requires separate journal and control databases")
	}
	if config.S3 != nil {
		return &nativeBackupRuntime{config: config, databasePath: path, controlDatabasePath: controlPath, slot: make(chan struct{}, 1), status: NativeBackupStatus{Enabled: true, State: "waiting", IntervalSeconds: int64(config.Interval / time.Second), TimeoutSeconds: int64(config.Timeout / time.Second), RetainCompleted: config.Retain}}, nil
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
	databases := []string{path, controlPath}
	for i, databasePath := range databases {
		if parent, err := filepath.EvalSymlinks(filepath.Dir(databasePath)); err == nil {
			databases[i] = filepath.Join(parent, filepath.Base(databasePath))
		}
	}
	if err := validateBackupDirectory(directory, native, databases); err != nil {
		return nil, err
	}
	info, err := os.Stat(directory)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("backup directory must be private to its owner")
	}
	config.Directory = directory
	return &nativeBackupRuntime{config: config, databasePath: path, controlDatabasePath: controlPath, slot: make(chan struct{}, 1), status: NativeBackupStatus{Enabled: true, State: "waiting", IntervalSeconds: int64(config.Interval / time.Second), TimeoutSeconds: int64(config.Timeout / time.Second), RetainCompleted: config.Retain}}, nil
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
	if err := lakeapi.ClaimLocalNativeBackupOwner(runtime.config.Directory, sourceID, writerID); err != nil {
		return err
	}
	if err := lakeapi.RemoveIncompleteNativeBackups(runtime.config.Directory); err != nil {
		return err
	}
	release, err := s.projectionGuard.exclusive(ctx)
	if err != nil {
		return err
	}
	var checkpoint *lakeapi.NativeCheckpoint
	var capturedAt string
	func() {
		defer release()
		checkpoint, err = s.stageNativeCheckpoint(ctx)
		if err == nil {
			capturedAt = checkpoint.CapturedAt
		}
	}()
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, checkpoint.Close()) }()
	if err := lakeapi.ValidateNativeBackupStorageIdentity(checkpoint.Storage, false); err != nil {
		return err
	}
	stage := checkpoint.Directory
	// Native projection and cleanup can resume now: only immutable hardlinks
	// and copied catalog/SQLite files remain in the private staging directory.
	if sourceID != checkpoint.SourceID {
		return errors.New("native backup source identity changed during capture")
	}
	id := "pc_backup_" + eventapi.NewID()
	partial := filepath.Join(runtime.config.Directory, "."+id+".incomplete")
	if err := os.Mkdir(partial, 0700); err != nil {
		return err
	}
	defer os.RemoveAll(partial)
	previousDirectory := ""
	previousFiles := map[string]lakeapi.BackupFile{}
	if previous := s.nativeBackupStatus().LatestBundle; filepath.IsLocal(previous) && filepath.Base(previous) == previous && strings.HasPrefix(previous, "pc_backup_") {
		candidate := filepath.Join(runtime.config.Directory, previous)
		if old, verifyErr := lakeapi.VerifyNativeBackup(ctx, candidate); verifyErr == nil && old.SourceID == sourceID {
			previousDirectory = candidate
			for _, file := range old.Files {
				previousFiles[file.Path] = file
			}
		}
	}
	manifest := lakeapi.NativeBackupManifest{Format: "porcupine-native-v2", ID: id, Version: Version, SourceID: sourceID, CreatedAt: now(), CapturedAt: capturedAt, SQLitePath: "acceptance.sqlite", ControlSQLitePath: "control.sqlite", DuckLakeDirectory: "ducklake", Catalogs: checkpoint.Catalogs, SourceStorage: checkpoint.Storage}
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
		if len(manifest.Files) >= lakeapi.NativeBackupFileLimit {
			return errors.New("native backup file inventory limit reached; reduce the small-file backlog before retrying")
		}
		file, err := lakeapi.HashBackupFile(ctx, path, filepath.ToSlash(relative))
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
			if err := lakeapi.CopyCheckpointFile(ctx, path, target); err != nil {
				return err
			}
		}
		manifest.Files = append(manifest.Files, file)
		return nil
	}); err != nil {
		return err
	}
	for _, path := range []string{filepath.Join(partial, "acceptance.sqlite"), filepath.Join(partial, "control.sqlite"), filepath.Join(partial, "ducklake", "registry.sqlite")} {
		if err := lakeapi.ValidateBackupSQLite(ctx, path); err != nil {
			return err
		}
	}
	if err := lakeapi.WriteBackupManifest(partial, manifest); err != nil {
		return err
	}
	if _, err := lakeapi.VerifyNativeBackup(ctx, partial); err != nil {
		return err
	}
	complete := filepath.Join(runtime.config.Directory, id)
	if err := os.Rename(partial, complete); err != nil {
		return err
	}
	if err := lakeapi.SyncBackupDirectory(runtime.config.Directory); err != nil {
		return err
	}
	if err := s.recordNativeBackupStatus(ctx, "ok", id, capturedAt); err != nil {
		return err
	}
	return lakeapi.PruneNativeBackups(runtime.config.Directory, sourceID, runtime.config.Retain, id)
}
