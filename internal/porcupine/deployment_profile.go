package porcupine

import (
	"errors"
	"net/url"
	"strings"
	"time"
)

type DeploymentProfile string

const (
	ProfileStandalone DeploymentProfile = "standalone"
	ProfileHosted     DeploymentProfile = "hosted"
)

func validateDeploymentProfile(c Config) error {
	switch c.Profile {
	case "", ProfileStandalone:
		if c.HostedFlushInterval != 0 {
			return errors.New("acceptance.flush_interval requires the hosted profile")
		}
		if c.Shared != nil || c.DuckLake != nil && c.DuckLake.PostgresURL != "" {
			return errors.New("PostgreSQL control and DuckLake metadata require the hosted profile")
		}
		return validateAcceptanceMode(c.AcceptanceMode)
	case ProfileHosted:
		switch c.AcceptanceMode {
		case AcceptanceModeBuffered:
			if c.DBPath != "" {
				return errors.New("a hosted storage.journal requires acceptance.mode durable")
			}
		case AcceptanceModeDurable:
			if strings.TrimSpace(c.DBPath) == "" || c.DBPath == ":memory:" {
				return errors.New("hosted durable acceptance requires a persistent storage.journal path")
			}
		default:
			return errors.New("hosted profile requires acceptance.mode buffered or durable")
		}
		if c.HostedFlushInterval != 0 && (c.HostedFlushInterval < time.Second || c.HostedFlushInterval > 30*time.Second) {
			return errors.New("acceptance.flush_interval must be between 1s and 30s")
		}
		if c.ControlDBPath != "" || c.NativeBackup != nil || c.ReceiptRetention != 0 {
			return errors.New("hosted profile cannot use local control, native backups or receipt retention")
		}
		if c.Shared == nil || c.DuckLake == nil || c.DuckLake.S3 == nil {
			return errors.New("hosted profile requires PostgreSQL control and analytical S3 storage")
		}
		db, err := url.Parse(c.Shared.DatabaseURL)
		if err != nil || db.Host == "" || (db.Scheme != "postgres" && db.Scheme != "postgresql") {
			return errors.New("hosted profile requires a PostgreSQL control URL")
		}
		if c.DuckLake.PostgresURL != c.Shared.DatabaseURL {
			return errors.New("hosted profile requires the same PostgreSQL URL for control and DuckLake metadata")
		}
		if c.DuckLake.RestoreWindow <= 0 || c.DuckLake.RestoreMaxReaderTime <= 0 || c.DuckLake.RestoreSafetyMargin <= 0 {
			return errors.New("hosted profile requires an explicit recovery window, maximum reader duration and recovery safety margin")
		}
		return nil
	default:
		return errors.New("profile must be standalone or hosted")
	}
}
