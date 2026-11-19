package porcupine

import (
	"context"
	"time"

	"github.com/olup/porcupine/internal/apierr"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/query"
)

const instanceProbeTimeout = time.Second

type DependencyStatus string

const (
	dependencyOK            DependencyStatus = "ok"
	dependencyUnavailable   DependencyStatus = "unavailable"
	dependencyNotConfigured DependencyStatus = "not_configured"
)

type InstanceChecks struct {
	ExternalAuthentication DependencyStatus `json:"external_authentication,omitempty" enum:"ok,unavailable" doc:"Configured external signing-key cache freshness. This check does not contact the issuer. Unavailable does not block public ingestion or native machine keys."`
	SQLite                 DependencyStatus `json:"sqlite" enum:"ok,unavailable,not_configured" doc:"Live SQLite connection check, bounded to one second."`
	DuckLake               DependencyStatus `json:"ducklake" enum:"ok,unavailable,not_configured" doc:"DuckLake registry identity and configured PostgreSQL metadata connection check, bounded to one second."`
	SharedControl          DependencyStatus `json:"shared_control,omitempty" enum:"ok,unavailable" doc:"Configured shared PostgreSQL control connection check, bounded to one second. Omitted for a local installation."`
	QueryExecution         DependencyStatus `json:"query_execution,omitempty" enum:"ok,unavailable" doc:"Shared analytical execution health on this worker, without a native query probe. Unavailable when draining or a project is quarantined; omitted in local mode."`
}

type InstanceConfiguration struct {
	ExternalAuthentication bool `json:"external_authentication" doc:"The integrated identity provider is replaced by an external access-token issuer. Native ingestion and machine keys remain supported."`
	Analytics              bool `json:"analytics" doc:"An embedded DuckDB/DuckLake analytical store is configured. Its live check is reported separately."`
	NativeS3Storage        bool `json:"native_s3_storage" doc:"DuckLake analytical object storage is configured. Does not probe bucket access or expose its endpoint, prefix or credentials."`
	QueryExports           bool `json:"query_exports" doc:"Private query export storage is configured. No remote storage request is performed."`
	WebhookPipelines       bool `json:"webhook_pipelines" doc:"The encrypted pipeline runtime is configured. Does not inspect or test project destinations."`
	S3QueryPipelines       bool `json:"s3_query_pipelines" doc:"The pipeline runtime and an analytical store are configured. Each pipeline still needs destination credentials and project grants."`
	EmailSignIn            bool `json:"email_sign_in" doc:"Email sign-in is configured. No verification email is sent."`
	OIDCSignIn             bool `json:"oidc_sign_in" doc:"Upstream OIDC sign-in is configured. No identity-provider request is performed."`
	SharedControl          bool `json:"shared_control" doc:"Shared PostgreSQL metadata and authentication storage is configured. No connection details are disclosed."`
}

type HostedIngestionStatus struct {
	DroppedRequests uint64 `json:"dropped_requests" doc:"Legacy count of live requests discarded due to output capacity. Always zero."`
	DroppedEvents   uint64 `json:"dropped_events" doc:"Legacy count of live events discarded due to output capacity. Always zero."`
}

type InstanceStatus struct {
	HostedIngestion *HostedIngestionStatus         `json:"hosted_ingestion,omitempty" doc:"Worker-local hosted ingestion counters. Visible only to instance administrators."`
	QueryExecution  *query.QueryExecutionStatus    `json:"query_execution,omitempty" doc:"Shared execution scope counts, uncertain permits, and worker-local project quarantine. Visible only to instance administrators; expiry does not guarantee that a partitioned native call stopped."`
	NativeBackup    *NativeBackupStatus            `json:"native_backup,omitempty" doc:"Configured native backup schedule and progress. Visible only to instance administrators."`
	DuckLakeCleanup *lakeapi.DuckLakeCleanupStatus `json:"ducklake_cleanup,omitempty" doc:"Snapshot and obsolete-file cleanup policy and progress. Visible only to instance administrators."`
	Status          string                         `json:"status" enum:"ok,degraded" doc:"Degraded when a live dependency check fails, shared analytical execution is unhealthy or draining, or administrator-visible DuckLake cleanup reports a failed/interrupted attempt. Unconfigured optional services do not degrade the instance."`
	Version         string                         `json:"version" doc:"Server build version, or dev for an unversioned build."`
	CheckedAt       string                         `json:"checked_at" doc:"UTC RFC3339 time when these checks started. This response is not cached."`
	Checks          InstanceChecks                 `json:"checks"`
	Configuration   InstanceConfiguration          `json:"configuration" doc:"Configured services, not health claims or authorization grants."`
}

type InstanceStatusOutput struct{ Body InstanceStatus }

func (s *Service) getInstanceStatus(ctx context.Context, _ *struct{}) (*InstanceStatusOutput, error) {
	select {
	case s.statusSlots <- struct{}{}:
		defer func() { <-s.statusSlots }()
	default:
		return nil, apierr.New(429, "an instance status check is already running")
	}

	result := InstanceStatus{
		Status: "ok", Version: Version, CheckedAt: time.Now().UTC().Format(time.RFC3339),
		Checks: InstanceChecks{SQLite: dependencyOK, DuckLake: dependencyNotConfigured},
		Configuration: InstanceConfiguration{
			ExternalAuthentication: s.externalAuthEnabled(),
			Analytics:              s.lake != nil, NativeS3Storage: s.lakeS3Storage, QueryExports: s.exports.Enabled(), WebhookPipelines: s.pipelines.Enabled(),
			S3QueryPipelines: s.lake != nil && s.pipelines.Enabled() &&
				(!s.pipelines.UsesSharedControl() || s.pipelines.SupportsScheduledS3()),
			EmailSignIn:   s.oauth != nil && s.oauth.config.Email != nil && s.oauth.config.Email.SendCode != nil,
			OIDCSignIn:    s.oauth != nil && s.oauth.browser != nil && s.oauth.browser.HasOIDC(),
			SharedControl: s.shared != nil,
		},
	}
	if s.externalAuthEnabled() {
		result.Checks.ExternalAuthentication = dependencyOK
		if !s.resources.external.SigningKeysAvailable(s.clock()) {
			result.Checks.ExternalAuthentication = dependencyUnavailable
			result.Status = "degraded"
		}
	}
	if s.shared != nil {
		health := s.queries.Status()
		result.Checks.QueryExecution = dependencyOK
		if health.Unavailable() {
			result.Checks.QueryExecution = dependencyUnavailable
			result.Status = "degraded"
		}
		if principal(ctx).Admin() {
			result.QueryExecution = &health
		}
	}
	if s.hosted != nil && principal(ctx).Admin() {
		ingestion := s.hosted.buffer.Snapshot()
		result.HostedIngestion = &HostedIngestionStatus{DroppedRequests: ingestion.DroppedRequests, DroppedEvents: ingestion.DroppedEvents}
	}
	if s.db == nil {
		result.Checks.SQLite = dependencyNotConfigured
	} else {
		probe, cancel := context.WithTimeout(ctx, instanceProbeTimeout)
		err := s.db.PingContext(probe)
		cancel()
		if err != nil {
			result.Checks.SQLite = dependencyUnavailable
			result.Status = "degraded"
		}
	}
	if s.shared == nil {
		probe, cancel := context.WithTimeout(ctx, instanceProbeTimeout)
		err := s.control.PingContext(probe)
		cancel()
		if err != nil {
			result.Checks.SQLite = dependencyUnavailable
			result.Status = "degraded"
		}
	}
	if s.shared != nil {
		result.Checks.SharedControl = dependencyUnavailable
		if s.control != nil {
			probe, cancel := context.WithTimeout(ctx, instanceProbeTimeout)
			err := s.control.PingContext(probe)
			cancel()
			if err == nil {
				result.Checks.SharedControl = dependencyOK
			}
		}
		if result.Checks.SharedControl != dependencyOK {
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
	if s.lake != nil && principal(ctx).Admin() {
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
	if s.backups != nil && principal(ctx).Admin() {
		result.NativeBackup = s.nativeBackupStatus()
		if result.NativeBackup != nil && (result.NativeBackup.State == "failed" || result.NativeBackup.State == "interrupted") {
			result.Status = "degraded"
		}
	}

	return &InstanceStatusOutput{Body: result}, nil
}
