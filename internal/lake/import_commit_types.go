package lake

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/olup/porcupine/internal/importer"
)

// ImportChunkKey distinguishes a byte range from final parser consumption at
// the same offset. Its ID is independent of attempt clocks and reserved IDs.
type ImportChunkKey struct {
	Scope              HostedProjectScope
	JobID, FileID      string
	Start, ExpectedEnd int64
	Phase              string
}

func (k ImportChunkKey) ID() string {
	raw, _ := json.Marshal(k)
	sum := sha256.Sum256(raw)
	return "import-" + hex.EncodeToString(sum[:])
}

// ImportFileCheckpoint contains metadata and an immutable private parser object.
// Tail bytes and serialized partial SHA blocks never enter the control store.
type ImportFileCheckpoint struct {
	Generation, Offset, LineNumber int64
	Completed                      bool
	Parser                         ImmutableObjectRef
	Counts                         importer.ImportCounts
	Samples                        []importer.ImportRowSample
}
type ImportCandidate struct {
	Position      int64
	Row           importer.Row
	InvalidReason string
}
type ImportChunkInput struct {
	Key                                                     ImportChunkKey
	TenantID, ManifestSHA256, SourceID, Format, InputSHA256 string
	Previous                                                ImportFileCheckpoint
	Parser                                                  ImmutableObjectRef
	LineNumber                                              int64
	Parsed                                                  importer.ImportCounts
	Samples                                                 []importer.ImportRowSample
	Failures                                                []importer.RowFailure
	Candidates                                              []ImportCandidate
}
type ImportRowOutcome struct {
	Position   int64
	UUID       string
	Reason     string
	EventID    int64
	Counts     importer.ImportCounts
	ReceivedAt time.Time
}
type ImportChunkOutcome struct {
	Key                              ImportChunkKey
	ManifestSHA256, SourceID, Format string
	Previous                         ImportFileCheckpoint
	Next                             ImportFileCheckpoint
	Delta                            importer.ImportCounts
	Rows                             []ImportRowOutcome
	Samples                          []importer.ImportRowSample
	AccountedAt                      time.Time
}
