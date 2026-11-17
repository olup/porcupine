package porcupine

import (
	"errors"
	"net/url"
)

type DeploymentProfile string

const (
	ProfileStandalone DeploymentProfile = "standalone"
	ProfileHosted     DeploymentProfile = "hosted"
)

func validateDeploymentProfile(c Config) error {
	switch c.Profile {
	case "", ProfileStandalone:
		if c.Shared != nil || c.DuckLake != nil && c.DuckLake.PostgresURL != "" {
			return errors.New("PostgreSQL control and DuckLake metadata require the hosted profile")
		}
		return validateAcceptanceMode(c.AcceptanceMode)
	case ProfileHosted:
		if c.AcceptanceMode != AcceptanceModeBuffered {
			return errors.New("hosted profile requires acceptance.mode buffered")
		}
		if c.DBPath != "" || c.ControlDBPath != "" || c.NativeBackup != nil || c.ReceiptRetention != 0 {
			return errors.New("hosted profile cannot use journal paths, local control, native backups or receipt retention")
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
