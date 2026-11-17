package porcupine

import (
	"errors"
	"github.com/olup/porcupine/internal/exportstore"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"net/url"
	"strings"
)

// ValidateConfig checks operator settings without opening a database, contacting
// an identity provider, or starting workers. File credentials are read by the caller.
func ValidateConfig(c Config) error {
	if err := validateSharedConfiguration(c); err != nil {
		return err
	}
	if _, err := normalizeInteractiveQueries(c.InteractiveQueries); err != nil {
		return err
	}
	if c.QueueMaxBytes < 0 {
		return errors.New("queue byte limit must be nonnegative")
	}
	if c.DBPath == "" {
		c.DBPath = "./data/porcupine.db"
	}
	if c.Shared == nil {
		if c.ControlDBPath == "" {
			c.ControlDBPath = localControlPath(c.DBPath)
		}
		if err := validateLocalDatabasePaths(c.DBPath, c.ControlDBPath); err != nil {
			return err
		}
	}
	if c.DuckLake != nil {
		cleanup := *c.DuckLake
		if err := lakeapi.NormalizeDuckLakeCleanup(&cleanup); err != nil {
			return err
		}
		if cleanup.MaintenanceInterval < 0 {
			return errors.New("DuckLake maintenance interval must be nonnegative")
		}
		if _, err := lakeapi.NormalizeDuckLakeS3(cleanup.S3); err != nil {
			return err
		}
	}
	if _, err := parseTrustedProxies(c.TrustedProxies); err != nil {
		return err
	}
	if _, err := parseAllowedOrigins(c.AllowedOrigins); err != nil {
		return err
	}
	if c.OAuth != nil {
		if err := ValidateOAuthConfig(*c.OAuth); err != nil {
			return err
		}
	}
	if _, err := newPipelineRuntime(c.Pipelines); err != nil {
		return err
	}
	if c.Exports != nil {
		if c.DuckLake == nil {
			return errors.New("exports require an analytical store")
		}
		if _, err := normalizeExportConfig(*c.Exports); err != nil {
			return err
		}
		if err := exportstore.ValidateConfig(c.Exports.S3); err != nil {
			return err
		}
	}
	if c.NativeBackup != nil {
		if _, err := normalizeNativeBackupConfig(c.NativeBackup, c.DBPath, c.ControlDBPath, c.DuckLake); err != nil {
			return err
		}
	}
	return nil
}

func parseAllowedOrigins(values []string) (map[string]bool, error) {
	origins := map[string]bool{}
	for _, origin := range values {
		origin = strings.TrimSpace(origin)
		u, err := url.Parse(origin)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || strings.Contains(origin, "*") || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
			return nil, errors.New("allowed origins must be exact http(s) origins without paths or wildcards")
		}
		origins[origin] = true
	}
	return origins, nil
}
