//go:build ducklake

package porcupine

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// stageCheckpoint returns a private, independent recovery staging directory.
// Its caller holds the exclusive projection guard through this method and
// removes the stage after copying it into a durable backup bundle.
func (s *duckLakeStore) stageCheckpoint(ctx context.Context, acceptance func(context.Context, string) error) (stage string, result error) {
	stage, err := os.MkdirTemp(s.root, ".checkpoint-")
	if err != nil {
		return "", err
	}
	owned := stage
	defer func() {
		if result != nil {
			os.RemoveAll(owned)
		}
	}()
	if err := acceptance(ctx, filepath.Join(stage, "acceptance.sqlite")); err != nil {
		return "", err
	}
	// Acquire only when every current reader has finished. Do not hold s.mu while
	// waiting, because releasing a lease needs that mutex too.
	for {
		if err := s.mu.Lock(ctx); err != nil {
			return "", err
		}
		if _, err := s.sourceIDLocked(ctx); err != nil {
			s.mu.Unlock()
			return "", err
		}
		busy := false
		for _, p := range s.projects {
			if len(p.leases) > 0 {
				busy = true
				break
			}
		}
		if !busy {
			break
		}
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	defer s.mu.Unlock()
	// SQLite has already been captured while native reads could continue.
	// Closing native handles checkpoints their catalogs. No native writer,
	// compactor or new reader can enter until the stage is complete.
	for id, p := range s.projects {
		err := p.db.Close()
		delete(s.projects, id)
		if err != nil {
			return "", err
		}
	}
	root := filepath.Join(stage, "ducklake")
	if err := os.Mkdir(root, 0700); err != nil {
		return "", err
	}
	if err := snapshotSQLite(ctx, s.registry, filepath.Join(root, "registry.sqlite")); err != nil {
		return "", err
	}
	rows, err := s.registry.QueryContext(ctx, "SELECT project_id FROM catalogs WHERE ready=1 ORDER BY project_id")
	if err != nil {
		return "", err
	}
	var projects []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return "", err
		}
		projects = append(projects, id)
	}
	err = errors.Join(rows.Err(), rows.Close())
	if err != nil {
		return "", err
	}
	for _, id := range projects {
		directory := s.projectDirectory(id)
		if _, err := os.Stat(filepath.Join(directory, "catalog.ducklake")); err != nil {
			return "", err
		}
		err := filepath.WalkDir(directory, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			relative, err := filepath.Rel(directory, path)
			if err != nil {
				return err
			}
			if entry.IsDir() && relative == "temp" {
				return filepath.SkipDir
			}
			target := filepath.Join(root, filepath.Base(directory), relative)
			if entry.IsDir() {
				return os.MkdirAll(target, 0700)
			}
			if !entry.Type().IsRegular() {
				return errors.New("checkpoint contains a non-regular native file")
			}
			if strings.HasPrefix(relative, "files"+string(os.PathSeparator)) && strings.HasSuffix(relative, ".parquet") {
				// Parquet data/delete files are immutable. A hardlink keeps the inode
				// alive even if a later cleanup or project deletion unlinks the source.
				return os.Link(path, target)
			}
			return copyCheckpointFile(ctx, path, target)
		})
		if err != nil {
			return "", err
		}
	}
	return stage, nil
}

// A process crash can leave staged hardlinks that would otherwise pin deleted
// payloads forever. The root writer lock proves there is no live owner.
func removeAbandonedCheckpoints(root string) error {
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), ".checkpoint-") {
			continue
		}
		suffix := strings.TrimPrefix(entry.Name(), ".checkpoint-")
		if len(suffix) == 0 || len(suffix) > 10 || strings.Trim(suffix, "0123456789") != "" {
			continue
		}
		if err := os.RemoveAll(filepath.Join(root, entry.Name())); err != nil {
			return err
		}
	}
	return nil
}
