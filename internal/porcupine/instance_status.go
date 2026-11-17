package porcupine

import (
	"context"
	"time"
)

const instanceProbeTimeout = time.Second

type DependencyStatus string

const (
	dependencyOK            DependencyStatus = "ok"
	dependencyUnavailable   DependencyStatus = "unavailable"
	dependencyNotConfigured DependencyStatus = "not_configured"
)

type InstanceChecks struct {
	SQLite             DependencyStatus `json:"sqlite" enum:"ok,unavailable" doc:"Live SQLite connection check, bounded to one second."`
	DuckLake           DependencyStatus `json:"ducklake" enum:"ok,unavailable,not_configured" doc:"DuckLake registry identity and configured PostgreSQL metadata connection check, bounded to one second."`
	SharedControl      DependencyStatus `json:"shared_control,omitempty" enum:"ok,unavailable" doc:"Configured shared PostgreSQL control connection check, bounded to one second. Omitted for a local installation."`
	WorkerRegistration DependencyStatus `json:"worker_registration,omitempty" enum:"ok,unavailable" doc:"Current worker registration and its dedicated PostgreSQL session lock, bounded to one second. Omitted for a local installation."`
}

type InstanceConfiguration struct {
	Analytics        bool `json:"analytics" doc:"An embedded DuckDB/DuckLake analytical store is configured. Its live check is reported separately."`
	NativeS3Storage  bool `json:"native_s3_storage" doc:"DuckLake analytical object storage is configured. Does not probe bucket access or expose its endpoint, prefix or credentials."`
	QueryExports     bool `json:"query_exports" doc:"Private query export storage is configured. No remote storage request is performed."`
	WebhookPipelines bool `json:"webhook_pipelines" doc:"The encrypted pipeline runtime is configured. Does not inspect or test project destinations."`
	S3QueryPipelines bool `json:"s3_query_pipelines" doc:"The pipeline runtime and an analytical store are configured. Each pipeline still needs destination credentials and project grants."`
	EmailSignIn      bool `json:"email_sign_in" doc:"Email sign-in is configured. No verification email is sent."`
	OIDCSignIn       bool `json:"oidc_sign_in" doc:"Upstream OIDC sign-in is configured. No identity-provider request is performed."`
	SharedControl    bool `json:"shared_control" doc:"Shared PostgreSQL metadata and authentication storage is configured. No connection details are disclosed."`
	OwnerRouting     bool `json:"owner_routing" doc:"Durable project-owner routing is configured. Its worker registration check is reported separately."`
}

type InstanceStatus struct {
	NativeBackup    *NativeBackupStatus    `json:"native_backup,omitempty" doc:"Configured native backup schedule and progress. Visible only to instance administrators."`
	DuckLakeCleanup *DuckLakeCleanupStatus `json:"ducklake_cleanup,omitempty" doc:"Snapshot and obsolete-file cleanup policy and progress. Visible only to instance administrators."`
	Status          string                 `json:"status" enum:"ok,degraded" doc:"Degraded when a live dependency check fails, or when administrator-visible DuckLake cleanup reports a failed/interrupted attempt. Unconfigured optional services do not degrade the instance."`
	Version         string                 `json:"version" doc:"Server build version, or dev for an unversioned build."`
	CheckedAt       string                 `json:"checked_at" doc:"UTC RFC3339 time when these checks started. This response is not cached."`
	Checks          InstanceChecks         `json:"checks"`
	Configuration   InstanceConfiguration  `json:"configuration" doc:"Configured services, not health claims or authorization grants."`
}

type InstanceStatusOutput struct{ Body InstanceStatus }

func (s *Service) getInstanceStatus(ctx context.Context, _ *struct{}) (*InstanceStatusOutput, error) {
	select {
	case s.statusSlots <- struct{}{}:
		defer func() { <-s.statusSlots }()
	default:
		return nil, apiError(429, "an instance status check is already running")
	}

	result := InstanceStatus{
		Status: "ok", Version: Version, CheckedAt: time.Now().UTC().Format(time.RFC3339),
		Checks: InstanceChecks{SQLite: dependencyOK, DuckLake: dependencyNotConfigured},
		Configuration: InstanceConfiguration{
			Analytics: s.lake != nil, NativeS3Storage: s.lakeS3Storage, QueryExports: s.exports != nil, WebhookPipelines: s.pipelines != nil,
			S3QueryPipelines: s.pipelines != nil && s.lake != nil,
			EmailSignIn:      s.oauth != nil && s.oauth.config.Email != nil,
			OIDCSignIn:       s.oauth != nil && s.oauth.oidc != nil,
			SharedControl:    s.shared != nil,
			OwnerRouting:     s.shared != nil,
		},
	}
	probe, cancel := context.WithTimeout(ctx, instanceProbeTimeout)
	err := s.db.PingContext(probe)
	cancel()
	if err != nil {
		result.Checks.SQLite = dependencyUnavailable
		result.Status = "degraded"
	}
	if s.shared != nil {
		result.Checks.SharedControl = dependencyUnavailable
		result.Checks.WorkerRegistration = dependencyUnavailable
		if s.control != nil {
			probe, cancel := context.WithTimeout(ctx, instanceProbeTimeout)
			err := s.control.PingContext(probe)
			cancel()
			if err == nil {
				result.Checks.SharedControl = dependencyOK
				state := s.shared.routing
				if state != nil && state.ctx != nil && state.ctx.Err() == nil && state.backendPID > 0 {
					probe, cancel := context.WithTimeout(ctx, instanceProbeTimeout)
					var registered bool
					// Inspect through the pool, rather than queuing behind a heartbeat on
					// the dedicated connection. That session holds only the worker lock.
					err := s.control.QueryRowContext(probe, `SELECT EXISTS(SELECT 1 FROM shared_workers WHERE worker_id=$1 AND registration_token=$2)
AND EXISTS(SELECT 1 FROM pg_locks WHERE pid=$3 AND locktype='advisory' AND granted)`, s.shared.config.WorkerID, state.token, state.backendPID).Scan(&registered)
					cancel()
					if err == nil && registered {
						result.Checks.WorkerRegistration = dependencyOK
					}
				}
			}
		}
		if result.Checks.SharedControl != dependencyOK || result.Checks.WorkerRegistration != dependencyOK {
			result.Status = "degraded"
		}
	}
	if s.lake != nil {
		probe, cancel := context.WithTimeout(ctx, instanceProbeTimeout)
		_, err := s.lake.SourceID(probe)
		cancel()
		result.Checks.DuckLake = dependencyOK
		if err != nil {
			result.Checks.DuckLake = dependencyUnavailable
			result.Status = "degraded"
		}
	}
	if s.lake != nil && principal(ctx).admin() {
		probe, cancel := context.WithTimeout(ctx, instanceProbeTimeout)
		status, err := s.lake.CleanupStatus(probe)
		cancel()
		if err == nil {
			result.DuckLakeCleanup = &status
			if status.FailedCatalogs > 0 {
				result.Status = "degraded"
			}
		} else {
			result.Status = "degraded"
		}
	}
	if s.backups != nil && principal(ctx).admin() {
		result.NativeBackup = s.nativeBackupStatus()
		if result.NativeBackup != nil && (result.NativeBackup.State == "failed" || result.NativeBackup.State == "interrupted") {
			result.Status = "degraded"
		}
	}

	return &InstanceStatusOutput{Body: result}, nil
}
