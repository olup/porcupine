package lake

import "time"

type NativeSourceTotals struct {
	Day          string `json:"day"`
	Revision     int64  `json:"revision"`
	Live, Import int64
}
type NativeCommitKind string

const (
	NativeCommitLive   NativeCommitKind = "live"
	NativeCommitImport NativeCommitKind = "import"
)

type NativeCommitEvidence struct {
	Kind                                            NativeCommitKind
	AcceptedImport                                  int64
	Import                                          *ImportChunkOutcome
	Scope                                           HostedProjectScope
	TenantID                                        string
	Sequence                                        int64
	OperationID, InputFingerprint, ProofFingerprint string
	FenceEpoch                                      int64
	AccountedAt                                     time.Time
	AcceptedLive                                    int64
	Effects                                         StagedLiveEffects
	Totals                                          []NativeSourceTotals
}
