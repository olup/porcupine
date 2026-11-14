package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func (s *Service) createNativeS3Backup(ctx context.Context, writerID string) (result error) {
	runtime := s.backups
	release, err := s.projectionGuard.exclusive(ctx)
	if err != nil {
		return err
	}
	var checkpoint *nativeCheckpoint
	func() {
		defer release()
		checkpoint, err = s.lake.stageCheckpoint(ctx, func(ctx context.Context, destination string) error {
			source, err := sql.Open("sqlite", (&url.URL{Scheme: "file", Path: runtime.databasePath}).String()+"?mode=ro&_pragma=busy_timeout(100)")
			if err != nil {
				return err
			}
			defer source.Close()
			return snapshotSQLite(ctx, source, destination)
		})
	}()
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, checkpoint.Close()) }()
	if err := validateNativeBackupStorageIdentity(checkpoint.Storage, false); err != nil {
		return err
	}
	destination, err := newNativeBackupS3(ctx, *runtime.config.S3, checkpoint.SourceID)
	if err != nil {
		return err
	}
	defer destination.Close()
	if err := destination.claimOwner(ctx, checkpoint.SourceID, writerID); err != nil {
		return err
	}
	if err := destination.cleanupIncomplete(ctx); err != nil {
		return err
	}
	id := "pc_backup_" + newID()
	published := false
	defer func() {
		if !published {
			cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			result = errors.Join(result, destination.remove(cleanup, id))
		}
	}()
	manifest := nativeBackupManifest{Format: "porcupine-native-v1", ID: id, Version: Version, SourceID: checkpoint.SourceID, CreatedAt: now(), CapturedAt: checkpoint.CapturedAt, SQLitePath: "acceptance.sqlite", DuckLakeDirectory: "ducklake", Catalogs: checkpoint.Catalogs, SourceStorage: checkpoint.Storage}
	for _, p := range []string{"acceptance.sqlite", "ducklake/registry.sqlite"} {
		if err := validateBackupSQLite(ctx, filepath.Join(checkpoint.Directory, filepath.FromSlash(p))); err != nil {
			return err
		}
	}
	paths := map[string]bool{}
	err = filepath.WalkDir(checkpoint.Directory, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return errors.New("backup stage contains a non-regular file")
		}
		if len(manifest.Files) >= nativeBackupFileLimit {
			return errors.New("native backup file inventory limit reached")
		}
		relative, err := filepath.Rel(checkpoint.Directory, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		body, err := os.Open(path)
		if err != nil {
			return err
		}
		defer body.Close()
		info, err := body.Stat()
		if err != nil {
			return err
		}
		file, err := destination.transfer(ctx, id, relative, body, info.Size())
		if err != nil {
			return err
		}
		paths[relative] = true
		manifest.Files = append(manifest.Files, file)
		return nil
	})
	if err != nil {
		return err
	}
	for _, object := range checkpoint.Objects {
		if err := ctx.Err(); err != nil {
			return err
		}
		path := object.RelativePath
		// Capture normalizes remote objects to the same bundle layout as local files.
		if !strings.HasPrefix(path, "ducklake/") || paths[path] || len(manifest.Files) >= nativeBackupFileLimit {
			return errors.New("invalid checkpoint object inventory")
		}
		body, err := checkpoint.OpenObject(ctx, object)
		if err != nil {
			return err
		}
		file, copyErr := destination.transfer(ctx, id, path, body, object.Bytes)
		closeErr := body.Close()
		if err := errors.Join(copyErr, closeErr); err != nil {
			return err
		}
		paths[path] = true
		manifest.Files = append(manifest.Files, file)
	}
	if err := destination.publish(ctx, manifest); err != nil {
		return err
	}
	published = true
	if err := s.recordNativeBackupStatus(ctx, "ok", id, checkpoint.CapturedAt); err != nil {
		return err
	}
	if err := destination.claimOwner(ctx, checkpoint.SourceID, writerID); err != nil {
		return err
	}
	return destination.prune(ctx, runtime.config.Retain, id)
}
