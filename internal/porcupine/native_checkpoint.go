package porcupine

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"
)

// Paths are relative to the completed bundle, independent of live storage.
type nativeBackupCatalog struct {
	ProjectID string `json:"project_id"`
	Path      string `json:"path"`
	DataRoot  string `json:"data_root"`
}

type nativeBackupStorageIdentity struct {
	Mode     string `json:"mode"`
	Bucket   string `json:"bucket,omitempty"`
	Prefix   string `json:"prefix,omitempty"`
	Region   string `json:"region,omitempty"`
	Endpoint string `json:"endpoint,omitempty"`
}

type nativeCheckpointObject struct {
	ProjectID    string
	RelativePath string
	Bytes        int64
	ETag         string
}

// Close releases snapshot protections and removes the private local stage.
// Remote objects stay pinned until the coordinator has copied and verified them.
type nativeCheckpoint struct {
	Directory  string
	SourceID   string
	CapturedAt string
	Storage    nativeBackupStorageIdentity
	Catalogs   []nativeBackupCatalog
	Objects    []nativeCheckpointObject
	openObject func(context.Context, nativeCheckpointObject) (io.ReadCloser, error)
	release    func() error
	once       sync.Once
	closeError error
}

func (c *nativeCheckpoint) OpenObject(ctx context.Context, object nativeCheckpointObject) (io.ReadCloser, error) {
	if c.openObject == nil {
		return nil, errors.New("checkpoint has no remote object reader")
	}
	return c.openObject(ctx, object)
}

func (c *nativeCheckpoint) Close() error {
	c.once.Do(func() {
		if c.release != nil {
			c.closeError = c.release()
		}
		if c.Directory != "" {
			c.closeError = errors.Join(c.closeError, os.RemoveAll(c.Directory))
		}
	})
	return c.closeError
}
