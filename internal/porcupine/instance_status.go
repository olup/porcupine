package porcupine

import (
	"context"
	"net/url"
	"strings"
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
	SQLite     DependencyStatus `json:"sqlite" enum:"ok,unavailable" doc:"Live SQLite connection check, bounded to one second."`
	DuckLake   DependencyStatus `json:"ducklake" enum:"ok,unavailable,not_configured" doc:"Experimental local archive identity check, bounded to one second."`
	ClickHouse DependencyStatus `json:"clickhouse" enum:"ok,unavailable,not_configured" doc:"Authenticated SELECT 1 in the configured database, bounded to one second. Does not validate every table or project reader."`
}

type InstanceConfiguration struct {
	QueryExports     bool `json:"query_exports" doc:"Private query export storage is configured. No remote storage request is performed."`
	WebhookPipelines bool `json:"webhook_pipelines" doc:"The encrypted pipeline runtime is configured. Does not inspect or test project destinations."`
	S3QueryPipelines bool `json:"s3_query_pipelines" doc:"The pipeline runtime and an analytical store are configured. Each pipeline still needs destination credentials and project grants."`
	EmailSignIn      bool `json:"email_sign_in" doc:"Email sign-in is configured. No verification email is sent."`
	OIDCSignIn       bool `json:"oidc_sign_in" doc:"Upstream OIDC sign-in is configured. No identity-provider request is performed."`
	S3Tiering        bool `json:"s3_tiering" doc:"A ClickHouse cold storage policy is configured. Its S3 disk and TTL progress are not probed."`
}

type InstanceStatus struct {
	Status        string                `json:"status" enum:"ok,degraded" doc:"Degraded when a live dependency check fails. Unconfigured optional services do not degrade the instance."`
	Version       string                `json:"version" doc:"Server build version, or dev for an unversioned build."`
	CheckedAt     string                `json:"checked_at" doc:"UTC RFC3339 time when these checks started. This response is not cached."`
	Checks        InstanceChecks        `json:"checks"`
	Configuration InstanceConfiguration `json:"configuration" doc:"Configured services, not health claims or authorization grants."`
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
		Checks: InstanceChecks{SQLite: dependencyOK, ClickHouse: dependencyNotConfigured, DuckLake: dependencyNotConfigured},
		Configuration: InstanceConfiguration{
			QueryExports: s.exports != nil, WebhookPipelines: s.pipelines != nil,
			S3QueryPipelines: s.pipelines != nil && (s.ch != nil || s.lake != nil),
			EmailSignIn:      s.oauth != nil && s.oauth.config.Email != nil,
			OIDCSignIn:       s.oauth != nil && s.oauth.oidc != nil,
			S3Tiering:        s.ch != nil && s.ch.config.StoragePolicy != "",
		},
	}
	probe, cancel := context.WithTimeout(ctx, instanceProbeTimeout)
	err := s.db.PingContext(probe)
	cancel()
	if err != nil {
		result.Checks.SQLite = dependencyUnavailable
		result.Status = "degraded"
	}
	if s.ch != nil {
		probe, cancel := context.WithTimeout(ctx, instanceProbeTimeout)
		data, err := s.ch.request(probe, s.ch.config.User, s.ch.config.Password, s.ch.config.Database,
			url.Values{"max_execution_time": {"1"}, "default_format": {"TabSeparated"}}, strings.NewReader("SELECT 1"), 128)
		cancel()
		result.Checks.ClickHouse = dependencyOK
		if err != nil || strings.TrimSpace(string(data)) != "1" {
			result.Checks.ClickHouse = dependencyUnavailable
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
	return &InstanceStatusOutput{Body: result}, nil
}
