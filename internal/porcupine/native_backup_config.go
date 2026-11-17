package porcupine

import (
	"errors"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"path/filepath"
	"strings"
	"time"
)

func normalizeNativeBackupConfig(c *lakeapi.NativeBackupConfig, database, controlDatabase string, lake *lakeapi.DuckLakeConfig) (lakeapi.NativeBackupConfig, error) {
	if lake != nil && lake.S3 != nil && c.S3 == nil {
		return lakeapi.NativeBackupConfig{}, errors.New("native local backups do not include remote analytical S3 objects; disable native backups for this storage mode")
	}
	if lake == nil || lake.Directory == "" || (database == ":memory:" || controlDatabase == ":memory:") || (c.Directory == "" && c.S3 == nil) {
		return lakeapi.NativeBackupConfig{}, errors.New("native backups require file-backed SQLite, DuckLake and one backup destination")
	}
	if c.Directory != "" && c.S3 != nil {
		return lakeapi.NativeBackupConfig{}, errors.New("configure exactly one native backup destination")
	}
	config := *c
	if config.Interval == 0 {
		config.Interval = 24 * time.Hour
	}
	if config.Timeout == 0 {
		config.Timeout = 5 * time.Minute
	}
	if config.Retain == 0 {
		config.Retain = 7
	}
	if config.Interval < time.Minute || config.Timeout < time.Second || config.Timeout > time.Hour || config.Retain < 1 || config.Retain > 1000 {
		return lakeapi.NativeBackupConfig{}, errors.New("invalid native backup schedule, timeout or retention")
	}
	if config.Directory != "" {
		directory, err := filepath.Abs(config.Directory)
		if err != nil {
			return lakeapi.NativeBackupConfig{}, err
		}
		native, err := filepath.Abs(lake.Directory)
		if err != nil {
			return lakeapi.NativeBackupConfig{}, err
		}
		journal, err := filepath.Abs(database)
		if err != nil {
			return lakeapi.NativeBackupConfig{}, err
		}
		control, err := filepath.Abs(controlDatabase)
		if err != nil {
			return lakeapi.NativeBackupConfig{}, err
		}
		if err := validateBackupDirectory(directory, native, []string{journal, control}); err != nil {
			return lakeapi.NativeBackupConfig{}, err
		}
	}
	if config.S3 != nil {
		normalized, err := lakeapi.NormalizeDuckLakeS3(config.S3)
		if err != nil {
			return lakeapi.NativeBackupConfig{}, errors.New("invalid native backup S3 configuration")
		}
		config.S3 = normalized
		source, err := lakeapi.NormalizeDuckLakeS3(lake.S3)
		if err != nil {
			return lakeapi.NativeBackupConfig{}, errors.New("invalid analytical S3 configuration for native backups")
		}
		if source != nil && source.Bucket == normalized.Bucket && (strings.HasPrefix(normalized.Prefix+"/", source.Prefix+"/") || strings.HasPrefix(source.Prefix+"/", normalized.Prefix+"/")) {
			return lakeapi.NativeBackupConfig{}, errors.New("backup S3 destination must not overlap analytical storage")
		}
	}
	return config, nil
}

func validateBackupDirectory(directory, native string, databases []string) error {
	relative, err := filepath.Rel(native, directory)
	if err != nil || relative == "." || filepath.IsLocal(relative) {
		return errors.New("backup destination must be outside the DuckLake directory")
	}
	if inverse, err := filepath.Rel(directory, native); err != nil || inverse == "." || filepath.IsLocal(inverse) {
		return errors.New("backup destination must not contain the DuckLake directory")
	}
	for _, database := range databases {
		if relative, err := filepath.Rel(directory, database); err != nil || relative == "." || filepath.IsLocal(relative) {
			return errors.New("backup destination must not contain a live SQLite database")
		}
	}
	return nil
}
