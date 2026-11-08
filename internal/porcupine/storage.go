package porcupine

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// normalizeStorageConfig applies deployment defaults only when tiering is enabled.
func normalizeStorageConfig(config *ClickHouseConfig) error {
	if config.StoragePolicy == "" {
		return nil
	}
	if config.HotStorageDays == 0 {
		config.HotStorageDays = 30
	}
	if config.ColdVolume == "" {
		config.ColdVolume = "cold"
	}
	if !clickHouseIdentifier.MatchString(config.StoragePolicy) || !clickHouseIdentifier.MatchString(config.ColdVolume) || config.HotStorageDays < 1 || config.HotStorageDays > 36500 {
		return errors.New("invalid ClickHouse storage policy configuration")
	}
	return nil
}

func validateStorageConfig(config ClickHouseConfig) error { return normalizeStorageConfig(&config) }

// applyStorage configures only MOVE TTLs. Empty deployment policy leaves local storage unchanged.
func (c *clickHouse) applyStorage(ctx context.Context, table string) error {
	config := c.config
	if err := normalizeStorageConfig(&config); err != nil {
		return err
	}
	if config.StoragePolicy == "" {
		return nil
	}
	qualified := strings.Split(table, ".")
	if len(qualified) != 2 || !clickHouseIdentifier.MatchString(qualified[0]) || !clickHouseIdentifier.MatchString(qualified[1]) {
		return errors.New("invalid ClickHouse storage table")
	}
	metadata, err := c.admin(ctx, "SELECT storage_policy,create_table_query FROM system.tables WHERE database=unhex('"+hex.EncodeToString([]byte(qualified[0]))+"') AND name=unhex('"+hex.EncodeToString([]byte(qualified[1]))+"') FORMAT JSONEachRow")
	if err != nil {
		return err
	}
	var current struct {
		Policy      string `json:"storage_policy"`
		CreateQuery string `json:"create_table_query"`
	}
	if json.Unmarshal(metadata, &current) != nil || current.Policy == "" || current.CreateQuery == "" {
		return errors.New("cannot inspect ClickHouse storage table")
	}
	if current.Policy != config.StoragePolicy {
		if _, err = c.admin(ctx, "ALTER TABLE "+table+" MODIFY SETTING storage_policy='"+config.StoragePolicy+"'"); err != nil {
			return err
		}
	}
	if storageTTLMatches(current.CreateQuery, config.HotStorageDays, config.ColdVolume) {
		return nil
	}
	// Recalculate existing part TTL metadata once per deployment change. Cold moves
	// run in the background; this never requests a move back to the hot volume.
	_, err = c.admin(ctx, fmt.Sprintf("ALTER TABLE %s MODIFY TTL toDateTime(fromUnixTimestamp64Milli(received_at_ms,'UTC')) + INTERVAL %d DAY TO VOLUME '%s' SETTINGS materialize_ttl_after_modify=1", table, config.HotStorageDays, config.ColdVolume))
	return err
}

var tableTTLClause = regexp.MustCompile(`(?is)\bTTL\s+(.+?)(?:\s+SETTINGS\s+|$)`)

func storageTTLMatches(createQuery string, days int, volume string) bool {
	matches := tableTTLClause.FindStringSubmatch(createQuery)
	if len(matches) != 2 {
		return false
	}
	compact := strings.Join(strings.Fields(matches[1]), "")
	compact = strings.ReplaceAll(compact, "`received_at_ms`", "received_at_ms")
	// SHOW CREATE canonicalizes INTERVAL n DAY to toIntervalDay(n).
	pattern := fmt.Sprintf(`^(?i:toDateTime\(fromUnixTimestamp64Milli\(received_at_ms,'UTC'\)\)\+(?:toIntervalDay\(%d\)|INTERVAL%dDAY)TOVOLUME)'%s'$`, days, days, regexp.QuoteMeta(volume))
	matched, err := regexp.MatchString(pattern, compact)
	return err == nil && matched
}
