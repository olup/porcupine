//go:build ducklake

package porcupine

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// The caller holds the exclusive projection guard until this method returns.
// Remote data remains protected from expiration and project deletion until Close.
func (s *duckLakeStore) stageCheckpoint(ctx context.Context, acceptance func(context.Context, string) error) (checkpoint *nativeCheckpoint, result error) {
	if err := s.cleanup.Lock(ctx); err != nil {
		return nil, err
	}
	cleanupLocked := true
	active := false
	var pinned []string
	var readers sync.RWMutex
	closed := false
	checkpoint = &nativeCheckpoint{}
	checkpoint.release = func() error {
		// A caller must close each object reader before closing its checkpoint.
		readers.Lock()
		defer readers.Unlock()
		closed = true
		_ = s.mu.Lock(context.Background())
		for _, id := range pinned {
			s.backupPins[id]--
			if s.backupPins[id] == 0 {
				delete(s.backupPins, id)
			}
		}
		if active {
			s.checkpointActive--
			active = false
		}
		s.mu.Unlock()
		if cleanupLocked {
			s.cleanup.Unlock()
			cleanupLocked = false
		}
		return nil
	}
	owned := checkpoint
	defer func() {
		if result != nil {
			result = errors.Join(result, owned.Close())
		}
	}()
	if err := s.mu.Lock(ctx); err != nil {
		return nil, err
	}
	source, err := s.sourceIDLocked(ctx)
	if err == nil {
		s.checkpointActive++
		active = true
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	checkpoint.Storage = nativeBackupStorageIdentity{Mode: "local"}
	if s.config.S3 != nil {
		c := s.config.S3
		checkpoint.Storage = nativeBackupStorageIdentity{Mode: "s3", Bucket: c.Bucket, Prefix: c.Prefix, Region: c.Region, Endpoint: c.Endpoint}
	}
	checkpoint.SourceID = source
	checkpoint.CapturedAt = time.Now().UTC().Format(time.RFC3339Nano)
	checkpoint.Directory, err = os.MkdirTemp(s.root, ".checkpoint-")
	if err != nil {
		return nil, err
	}
	if err := acceptance(ctx, filepath.Join(checkpoint.Directory, "acceptance.sqlite")); err != nil {
		return nil, err
	}
	// Releasing a lease needs s.mu too, so wait without holding it.
	for {
		if err := s.mu.Lock(ctx); err != nil {
			return nil, err
		}
		if _, err := s.sourceIDLocked(ctx); err != nil {
			s.mu.Unlock()
			return nil, err
		}
		busy := len(s.opening) > 0 || s.deletionActive
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
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	// A failed deletion may already have removed some remote files. Its durable
	// fence must be resolved before a complete checkpoint can be captured.
	var deleting int
	if err := s.registry.QueryRowContext(ctx, "SELECT count(*) FROM deleting_projects").Scan(&deleting); err != nil || deleting != 0 {
		s.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return nil, errors.New("native checkpoint requires pending project deletions to finish")
	}
	err = s.captureCheckpointLocked(ctx, checkpoint)
	if s.remote != nil {
		for _, catalog := range checkpoint.Catalogs {
			pinned = append(pinned, catalog.ProjectID)
		}
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if s.remote == nil {
		if err := verifyCheckpointReferences(ctx, checkpoint); err != nil {
			return nil, err
		}
		// Local immutable hardlinks survive cleanup/deletion without live pins.
		if err := checkpoint.release(); err != nil {
			return nil, err
		}
		checkpoint.release = nil
		return checkpoint, nil
	}
	// Project pins and the cleanup guard remain owned by the checkpoint. The
	// global mutex is free throughout LIST and GET, including slow endpoints.
	if err := s.inventoryCheckpoint(ctx, checkpoint); err != nil {
		return nil, err
	}
	if err := verifyCheckpointReferences(ctx, checkpoint); err != nil {
		return nil, err
	}
	objects := make(map[string]nativeCheckpointObject, len(checkpoint.Objects))
	prefixes := make(map[string]string, len(checkpoint.Catalogs))
	for _, catalog := range checkpoint.Catalogs {
		prefixes[catalog.ProjectID] = s.remote.projectPrefix(catalog.ProjectID)
	}
	for _, object := range checkpoint.Objects {
		objects[object.RelativePath] = object
	}
	checkpoint.openObject = func(ctx context.Context, object nativeCheckpointObject) (io.ReadCloser, error) {
		readers.RLock()
		if closed {
			readers.RUnlock()
			return nil, errors.New("native checkpoint is closed")
		}
		captured, exists := objects[object.RelativePath]
		root := "ducklake/" + projectLakeDigest(object.ProjectID) + "/files/"
		relative := strings.TrimPrefix(object.RelativePath, root)
		prefix, projectExists := prefixes[object.ProjectID]
		if !exists || captured != object || !projectExists || relative == object.RelativePath || !validCheckpointRelative(relative) {
			readers.RUnlock()
			return nil, errors.New("object is outside the native checkpoint inventory")
		}
		response, err := s.remote.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.remote.config.Bucket), Key: aws.String(prefix + relative), IfMatch: aws.String(object.ETag)}, func(options *s3.Options) {
			// Backup payloads may exceed the short analytical request timeout.
			// Reuse transport and credentials while respecting the backup's total
			// deadline, without changing ordinary analytical operations.
			if deadline, bounded := ctx.Deadline(); bounded {
				if original, standard := options.HTTPClient.(*http.Client); standard {
					client := *original
					client.Timeout = max(time.Until(deadline), time.Nanosecond)
					options.HTTPClient = &client
				}
			}
		})
		if err != nil {
			readers.RUnlock()
			return nil, analyticalS3Error(ctx)
		}
		if response.ContentLength == nil || *response.ContentLength != object.Bytes || aws.ToString(response.ETag) != object.ETag {
			response.Body.Close()
			readers.RUnlock()
			return nil, errors.New("native checkpoint object changed since capture")
		}
		return &checkpointObjectReader{ReadCloser: response.Body, release: readers.RUnlock}, nil
	}
	return checkpoint, nil
}

// captureCheckpointLocked closes idle engines and copies catalogs before any
// writer or new reader can enter. Remote pins do not consume resident engines.
func (s *duckLakeStore) captureCheckpointLocked(ctx context.Context, checkpoint *nativeCheckpoint) error {
	for id, p := range s.projects {
		err := p.db.Close()
		delete(s.projects, id)
		if err != nil {
			return err
		}
	}
	root := filepath.Join(checkpoint.Directory, "ducklake")
	if err := os.Mkdir(root, 0700); err != nil {
		return err
	}
	if err := snapshotSQLite(ctx, s.registry, filepath.Join(root, "registry.sqlite")); err != nil {
		return err
	}
	rows, err := s.registry.QueryContext(ctx, "SELECT project_id FROM catalogs WHERE ready=1 ORDER BY project_id")
	if err != nil {
		return err
	}
	var projects []string
	metadataBytes := 0
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		metadataBytes += len(id)*6 + 1024
		if id == "" || len(id) > 1000 || metadataBytes > nativeBackupManifestLimit-(1<<20) || len(projects) >= nativeBackupFileLimit-2 {
			rows.Close()
			return errors.New("native checkpoint catalog inventory limit reached")
		}
		projects = append(projects, id)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	for _, id := range projects {
		directory := s.projectDirectory(id)
		catalog := nativeBackupCatalog{ProjectID: id, Path: "ducklake/" + projectLakeDigest(id) + "/catalog.ducklake", DataRoot: "ducklake/" + projectLakeDigest(id) + "/files"}
		checkpoint.Catalogs = append(checkpoint.Catalogs, catalog)
		if s.remote != nil {
			if s.backupPins == nil {
				s.backupPins = make(map[string]int)
			}
			s.backupPins[id]++
			// The release list is derived from copied catalogs even on failure.
			if err := os.MkdirAll(filepath.Join(checkpoint.Directory, filepath.FromSlash(catalog.DataRoot)), 0700); err != nil {
				return err
			}
			info, err := os.Lstat(filepath.Join(directory, "catalog.ducklake"))
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return errors.New("checkpoint catalog is not a regular native file")
			}
			if err := copyCheckpointFile(ctx, filepath.Join(directory, "catalog.ducklake"), filepath.Join(checkpoint.Directory, filepath.FromSlash(catalog.Path))); err != nil {
				return err
			}
			continue
		}
		if _, err := os.Stat(filepath.Join(directory, "catalog.ducklake")); err != nil {
			return err
		}
		if err := filepath.WalkDir(directory, func(path string, entry fs.DirEntry, walkErr error) error {
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
				return os.Link(path, target)
			}
			return copyCheckpointFile(ctx, path, target)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *duckLakeStore) inventoryCheckpoint(ctx context.Context, checkpoint *nativeCheckpoint) error {
	// Reserve room for catalog metadata and the manifest envelope. The final
	// coordinator checks the complete serialized manifest before publication.
	metadataBytes := 0
	for _, catalog := range checkpoint.Catalogs {
		metadataBytes += len(catalog.ProjectID)*6 + 1024
	}
	seen := make(map[string]struct{})
	for _, catalog := range checkpoint.Catalogs {
		prefix := s.remote.projectPrefix(catalog.ProjectID)
		request := &s3.ListObjectsV2Input{Bucket: aws.String(s.remote.config.Bucket), Prefix: aws.String(prefix), MaxKeys: aws.Int32(1000)}
		tokens := make(map[string]struct{})
		for {
			page, err := s.remote.client.ListObjectsV2(ctx, request)
			if err != nil {
				return analyticalS3Error(ctx)
			}
			for _, object := range page.Contents {
				key, etag := aws.ToString(object.Key), aws.ToString(object.ETag)
				relative := strings.TrimPrefix(key, prefix)
				if relative == key || !validCheckpointRelative(relative) || object.Size == nil || *object.Size < 0 || etag == "" || len(etag) > 1024 || strings.ContainsAny(etag, "\r\n") {
					return errors.New("invalid native checkpoint object inventory")
				}
				archive := catalog.DataRoot + "/" + relative
				if _, exists := seen[archive]; exists {
					return errors.New("duplicate native checkpoint object inventory")
				}
				seen[archive] = struct{}{}
				metadataBytes += len(archive)*6 + len(catalog.ProjectID)*6 + len(etag)*6 + 256
				if len(checkpoint.Objects)+len(checkpoint.Catalogs)+2 >= nativeBackupFileLimit || metadataBytes > nativeBackupManifestLimit-(1<<20) {
					return errors.New("native checkpoint object inventory limit reached; reduce the small-file backlog before retrying")
				}
				checkpoint.Objects = append(checkpoint.Objects, nativeCheckpointObject{ProjectID: catalog.ProjectID, RelativePath: archive, Bytes: *object.Size, ETag: etag})
			}
			if !aws.ToBool(page.IsTruncated) {
				break
			}
			token := aws.ToString(page.NextContinuationToken)
			_, repeated := tokens[token]
			if token == "" || len(token) > 8192 || repeated || len(page.Contents) == 0 || len(tokens) >= nativeBackupFileLimit {
				return errors.New("incomplete native checkpoint object inventory")
			}
			tokens[token] = struct{}{}
			request.ContinuationToken = page.NextContinuationToken
		}
	}
	return nil
}

func validCheckpointRelative(relative string) bool {
	return relative != "." && len(relative) <= 4096 && !strings.ContainsAny(relative, "\\\x00") && filepath.IsLocal(filepath.FromSlash(relative)) && filepath.ToSlash(filepath.Clean(filepath.FromSlash(relative))) == relative
}

type checkpointObjectReader struct {
	io.ReadCloser
	release    func()
	once       sync.Once
	closeError error
}

func (r *checkpointObjectReader) Close() error {
	r.once.Do(func() {
		r.closeError = r.ReadCloser.Close()
		r.release()
	})
	return r.closeError
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
