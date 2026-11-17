// Package output owns export and pipeline jobs, attempts, artifacts and retention.
package output

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	"github.com/olup/porcupine/internal/apierr"
	"github.com/olup/porcupine/internal/auth"
	"github.com/olup/porcupine/internal/exportstore"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/usage"
)

// ExportDependencies are the surrounding resource authorization and native admission
// boundaries. Export state and execution remain entirely inside ExportJobs.
type ExportDependencies struct {
	Journal, Control            *sql.DB
	Clock                       func() time.Time
	Reader                      ExportReader
	Usage                       *usage.Recorder
	Issuer                      string
	LoadActor                   func(context.Context, string) (auth.Principal, error)
	CurrentActor                func(context.Context, auth.Principal) (auth.Principal, error)
	RequireActive, RequireOwner func(context.Context, string) error
	BeginActivity               func(context.Context, string) (context.Context, func(), bool)
	Acquire                     func(context.Context) (func(), bool)
}

type ExportJobs struct {
	db, control                 *sql.DB
	clock                       func() time.Time
	lake                        ExportReader
	usage                       *usage.Recorder
	issuer                      string
	loadActor                   func(context.Context, string) (auth.Principal, error)
	currentActor                func(context.Context, auth.Principal) (auth.Principal, error)
	requireActive, requireOwner func(context.Context, string) error
	beginActivity               func(context.Context, string) (context.Context, func(), bool)
	acquire                     func(context.Context) (func(), bool)
	config                      ExportConfig
	store                       *exportstore.Store
	cancel                      context.CancelFunc
	done                        chan struct{}
	wake                        chan struct{}
	activeMu                    sync.Mutex
	activeJob                   string
	activeCancel                context.CancelFunc
}

// NewSQLiteExports also constructs a read/cleanup domain when exports are disabled.
// It uses the schema installed by the journal's numbered migrations.
func NewSQLiteExports(d ExportDependencies, config *ExportConfig) (*ExportJobs, error) {
	e := &ExportJobs{db: d.Journal, control: d.Control, clock: d.Clock, lake: d.Reader, usage: d.Usage, issuer: d.Issuer, loadActor: d.LoadActor, currentActor: d.CurrentActor, requireActive: d.RequireActive, requireOwner: d.RequireOwner, beginActivity: d.BeginActivity, acquire: d.Acquire, wake: make(chan struct{}, 1)}
	if e.clock == nil {
		e.clock = time.Now
	}
	if config != nil {
		c, err := NormalizeExportConfig(*config)
		if err != nil {
			return nil, err
		}
		store, err := exportstore.New(context.Background(), c.S3)
		if err != nil {
			return nil, errors.New("invalid S3 export configuration")
		}
		e.config, e.store = c, store
	}
	return e, nil
}
func (e *ExportJobs) Enabled() bool        { return e != nil && e.store != nil }
func (e *ExportJobs) Config() ExportConfig { return e.config }
func (e *ExportJobs) Close() {
	e.Stop()
	if e.store != nil {
		e.store.Close()
	}
}

type ExportRequest struct {
	IdempotencyKey, SQL, Format string
	Parameters                  map[string]string
}
type ExportPage struct {
	Jobs       []ExportJob
	NextOffset *int
}
type ExportFilePage struct {
	Files         []ExportFile
	Limit, Offset int
	NextOffset    *int
}

func apiError(status int, message string) error { return apierr.New(status, message) }
func hash(v string) string                      { b := sha256.Sum256([]byte(v)); return hex.EncodeToString(b[:]) }
func authenticationUnavailable(err error) bool {
	var status interface{ GetStatus() int }
	return errors.Is(err, auth.ErrUnavailable) || errors.Is(err, auth.ErrExternalKeysUnavailable) || errors.As(err, &status) && status.GetStatus() == 503
}

// ExportReader holds its native lease and query permit until Export returns.
type ExportReader interface {
	Export(context.Context, string, string, map[string]string, *lakeapi.AnalyticalWindow, int64, lakeapi.ExportStreamCallbacks) (lakeapi.ExportStreamResult, error)
}
