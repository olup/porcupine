package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

const (
	catalogRefreshInterval = time.Hour
	catalogSummaryMaxRows  = 20000
)

var (
	errCatalogRefreshClaimLost = errors.New("event catalog refresh claim lost")
	errCatalogProjectFenced    = errHostedProjectFenced
)

type catalogSummaryStore struct {
	db     *sql.DB
	hosted bool
}

type catalogSummaryState struct {
	Snapshot    lakeapi.ExportSnapshot
	RefreshedAt time.Time
	Status      string
}

type catalogRefreshClaim struct {
	ProjectID string
	Snapshot  lakeapi.ExportSnapshot
	token     string
	failures  int
}

const catalogProjectActive = `NOT EXISTS(SELECT 1 FROM project_deletions d WHERE d.project_id=p.id)
 AND NOT EXISTS(SELECT 1 FROM tenant_deletions d WHERE d.tenant_id=p.tenant_id)`

func catalogState(snapshot lakeapi.ExportSnapshot, refreshedMS, eligibleMS int64, token string, failures int, now time.Time) catalogSummaryState {
	state := catalogSummaryState{Snapshot: snapshot}
	if refreshedMS != 0 {
		state.RefreshedAt = time.UnixMilli(refreshedMS).UTC()
	}
	switch {
	case token != "" && eligibleMS > now.UnixMilli():
		state.Status = "running"
	case failures > 0:
		state.Status = "retrying"
	case eligibleMS <= now.UnixMilli():
		state.Status = "queued"
	default:
		state.Status = "current"
	}
	return state
}

// State and rows must come from the same database snapshot, including when the
// page is empty. The bounded subquery can use catalog_summary_page directly.
func (s catalogSummaryStore) page(ctx context.Context, input lakeapi.EventCatalogInput, now time.Time) (lakeapi.EventCatalog, catalogSummaryState, error) {
	rows, err := s.db.QueryContext(ctx, `WITH state AS (
 SELECT c.catalog_id,c.snapshot_version,c.refreshed_at_ms,c.eligible_at_ms,c.lease_token,c.failures
 FROM catalog_refresh_state c JOIN projects p ON p.id=c.project_id WHERE c.project_id=$1 AND `+catalogProjectActive+`
), page AS (
 SELECT event_name,last_id,event_count,first_received_at,last_received_at,latest_event_uuid,latest_event_timestamp
 FROM catalog_summary WHERE project_id=$1 AND EXISTS(SELECT 1 FROM state WHERE catalog_id<>'')
 ORDER BY last_id DESC,event_name LIMIT $2 OFFSET $3
)
SELECT state.*,COALESCE(page.event_name,''),COALESCE(CAST(page.event_count AS TEXT),''),
 COALESCE(page.first_received_at,''),COALESCE(page.last_received_at,''),
 COALESCE(page.latest_event_uuid,''),COALESCE(page.latest_event_timestamp,'')
FROM state LEFT JOIN page ON 1=1 ORDER BY page.last_id DESC,page.event_name`, input.ID, input.Limit+1, input.Offset)
	if err != nil {
		return lakeapi.EventCatalog{}, catalogSummaryState{}, err
	}
	defer rows.Close()
	page := lakeapi.EventCatalog{
		Events:     []lakeapi.EventCatalogEvent{},
		Properties: []lakeapi.EventCatalogProperty{},
		Sampling:   lakeapi.EventCatalogSampling{SampleEventLimit: 1000, KeysPerEventLimit: 200, MaxPropertyNameBytes: 200, PropertyLimit: input.PropertyLimit},
	}
	var snapshot lakeapi.ExportSnapshot
	var refreshed, eligible int64
	var token string
	var failures int
	var state catalogSummaryState
	for rows.Next() {
		var event lakeapi.EventCatalogEvent
		if err := rows.Scan(&snapshot.CatalogID, &snapshot.Version, &refreshed, &eligible, &token, &failures,
			&event.Event, &event.Count, &event.FirstReceivedAt, &event.LastReceivedAt, &event.LatestEventUUID, &event.LatestEventTimestamp); err != nil {
			return lakeapi.EventCatalog{}, catalogSummaryState{}, err
		}
		state = catalogState(snapshot, refreshed, eligible, token, failures, now)
		if event.Count != "" {
			page.Events = append(page.Events, event)
		}
	}
	if len(page.Events) > input.Limit {
		page.Events = page.Events[:input.Limit]
		if next := input.Offset + input.Limit; next <= 10000 {
			page.NextOffset = &next
		} else {
			page.Sampling.PaginationTruncated = true
		}
	}
	return page, state, rows.Err()
}

func (s catalogSummaryStore) lockActiveProject(ctx context.Context, tx *sql.Tx, project string) error {
	if s.hosted {
		_, err := lockActiveHostedProject(ctx, tx, project)
		return err
	}
	var active bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM projects p WHERE p.id=$1 AND `+catalogProjectActive+`)`, project).Scan(&active)
	if err == nil && !active {
		return errCatalogProjectFenced
	}
	return err
}

// A discovery request only seeds the row. A manual request can advance the
// next eligible time, while an unexpired claim covers concurrent requests.
func (s catalogSummaryStore) request(ctx context.Context, project string, now time.Time, force bool) (catalogSummaryState, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return catalogSummaryState{}, err
	}
	defer tx.Rollback()
	if err := s.lockActiveProject(ctx, tx, project); err != nil {
		return catalogSummaryState{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO catalog_refresh_state(project_id,eligible_at_ms) VALUES($1,$2)
ON CONFLICT(project_id) DO UPDATE SET eligible_at_ms=excluded.eligible_at_ms,failures=0
WHERE $3=1 AND catalog_refresh_state.lease_token='' AND catalog_refresh_state.eligible_at_ms>excluded.eligible_at_ms`, project, now.UnixMilli(), force); err != nil {
		return catalogSummaryState{}, err
	}
	var snapshot lakeapi.ExportSnapshot
	var refreshed, eligible int64
	var token string
	var failures int
	if err := tx.QueryRowContext(ctx, `SELECT catalog_id,snapshot_version,refreshed_at_ms,eligible_at_ms,lease_token,failures
FROM catalog_refresh_state WHERE project_id=$1`, project).Scan(&snapshot.CatalogID, &snapshot.Version, &refreshed, &eligible, &token, &failures); err != nil {
		return catalogSummaryState{}, err
	}
	return catalogState(snapshot, refreshed, eligible, token, failures, now), tx.Commit()
}

// Due jobs and abandoned claims use the same index. A claim locks only the
// refresh row, leaving admission and deletion free to lock project rows.
func (s catalogSummaryStore) claim(ctx context.Context, now time.Time, lease time.Duration) (catalogRefreshClaim, error) {
	claim := catalogRefreshClaim{token: eventapi.NewID()}
	lock := ""
	if s.hosted {
		lock = " FOR UPDATE OF c SKIP LOCKED"
	}
	err := s.db.QueryRowContext(ctx, `WITH due AS (
 SELECT c.project_id FROM catalog_refresh_state c JOIN projects p ON p.id=c.project_id
 WHERE c.eligible_at_ms<=$1 AND `+catalogProjectActive+`
 ORDER BY c.eligible_at_ms,c.project_id LIMIT 1`+lock+`
)
UPDATE catalog_refresh_state SET lease_token=$2,eligible_at_ms=$3
WHERE project_id=(SELECT project_id FROM due) AND eligible_at_ms<=$1
RETURNING project_id,catalog_id,snapshot_version,failures`, now.UnixMilli(), claim.token, now.Add(lease).UnixMilli()).Scan(&claim.ProjectID, &claim.Snapshot.CatalogID, &claim.Snapshot.Version, &claim.failures)
	return claim, err
}

const catalogClaimPredicate = `project_id=$1 AND lease_token=$2 AND eligible_at_ms>$3 AND catalog_id=$4 AND snapshot_version=$5`

func (claim catalogRefreshClaim) args(now time.Time) []any {
	return []any{claim.ProjectID, claim.token, now.UnixMilli(), claim.Snapshot.CatalogID, claim.Snapshot.Version}
}

func (s catalogSummaryStore) complete(ctx context.Context, claim catalogRefreshClaim, result lakeapi.CatalogSnapshotRead, now time.Time) error {
	if len(result.Rows) > catalogSummaryMaxRows {
		return lakeapi.ErrCatalogSummaryTooLarge
	}
	if result.Snapshot.CatalogID == "" || result.Snapshot.Version < 0 ||
		(!result.Replace && (result.Snapshot.CatalogID != claim.Snapshot.CatalogID ||
			result.Snapshot.Version < claim.Snapshot.Version || result.Snapshot == claim.Snapshot && len(result.Rows) != 0)) {
		return errors.New("invalid event catalog refresh checkpoint")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := s.lockActiveProject(ctx, tx, claim.ProjectID); err != nil {
		return err
	}
	var count int
	err = tx.QueryRowContext(ctx, `UPDATE catalog_refresh_state
SET catalog_id=$6,snapshot_version=$7,refreshed_at_ms=$3,eligible_at_ms=$8,lease_token='',failures=0
WHERE `+catalogClaimPredicate+` RETURNING summary_rows`, append(claim.args(now), result.Snapshot.CatalogID, result.Snapshot.Version, now.Add(catalogRefreshInterval).UnixMilli())...).Scan(&count)
	if errors.Is(err, sql.ErrNoRows) {
		return errCatalogRefreshClaimLost
	}
	if err != nil {
		return err
	}
	if result.Replace {
		if _, err := tx.ExecContext(ctx, `DELETE FROM catalog_summary WHERE project_id=$1`, claim.ProjectID); err != nil {
			return err
		}
		count = 0
	}
	if len(result.Rows) != 0 {
		count, err = writeCatalogRows(ctx, tx, claim.ProjectID, result.Rows, count)
		if err != nil {
			return err
		}
	}
	if result.Replace || len(result.Rows) != 0 {
		if _, err := tx.ExecContext(ctx, `UPDATE catalog_refresh_state SET summary_rows=$2 WHERE project_id=$1`, claim.ProjectID, count); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Earlier imports can add counts without moving the last-event pointer. Use
// durable IDs, independent of client and received timestamps, for both ends.
func writeCatalogRows(ctx context.Context, tx *sql.Tx, project string, rows []lakeapi.CatalogAggregate, numRows int) (int, error) {
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO catalog_summary
(project_id,event_name,first_id,last_id,event_count,first_received_at,last_received_at,latest_event_uuid,latest_event_timestamp)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
ON CONFLICT(project_id,event_name) DO UPDATE SET
 first_id=CASE WHEN excluded.first_id<catalog_summary.first_id THEN excluded.first_id ELSE catalog_summary.first_id END,
 last_id=CASE WHEN excluded.last_id>catalog_summary.last_id THEN excluded.last_id ELSE catalog_summary.last_id END,
 event_count=catalog_summary.event_count+excluded.event_count,
 first_received_at=CASE WHEN excluded.first_id<catalog_summary.first_id THEN excluded.first_received_at ELSE catalog_summary.first_received_at END,
 last_received_at=CASE WHEN excluded.last_id>catalog_summary.last_id THEN excluded.last_received_at ELSE catalog_summary.last_received_at END,
 latest_event_uuid=CASE WHEN excluded.last_id>catalog_summary.last_id THEN excluded.latest_event_uuid ELSE catalog_summary.latest_event_uuid END,
 latest_event_timestamp=CASE WHEN excluded.last_id>catalog_summary.last_id THEN excluded.latest_event_timestamp ELSE catalog_summary.latest_event_timestamp END
RETURNING event_count=$5`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	for _, row := range rows {
		event := row.Event
		count, err := strconv.ParseInt(event.Count, 10, 64)
		if err != nil || count < 1 || row.FirstID < 1 || row.LastID < row.FirstID {
			return 0, errors.New("invalid event catalog aggregate")
		}
		if !catalogTextFits(event.Event, 500) || !catalogTextFits(event.LatestEventUUID, 256) ||
			!catalogTextFits(event.FirstReceivedAt, 64) || !catalogTextFits(event.LastReceivedAt, 64) || !catalogTextFits(event.LatestEventTimestamp, 64) {
			return 0, lakeapi.ErrCatalogSummaryTooLarge
		}
		var inserted bool
		if err := stmt.QueryRowContext(ctx, project, event.Event, row.FirstID, row.LastID, count,
			event.FirstReceivedAt, event.LastReceivedAt, event.LatestEventUUID, event.LatestEventTimestamp).Scan(&inserted); err != nil {
			return 0, err
		}
		if inserted {
			numRows++
			if numRows > catalogSummaryMaxRows {
				return 0, lakeapi.ErrCatalogSummaryTooLarge
			}
		}
	}
	return numRows, nil
}

func catalogTextFits(text string, maxBytes int) bool {
	return len(text) <= maxBytes && utf8.ValidString(text) && !strings.ContainsRune(text, 0)
}

func (s catalogSummaryStore) fail(ctx context.Context, claim catalogRefreshClaim, now time.Time, discard bool) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := s.lockActiveProject(ctx, tx, claim.ProjectID); err != nil {
		return err
	}
	delay := min(time.Minute<<min(claim.failures, 6), catalogRefreshInterval)
	failures := min(claim.failures+1, 6)
	checkpoint := ""
	if discard {
		// The old rows stay hidden until a complete rebuild fits or the project
		// is deleted. Back off so a large catalog does not rebuild in a loop.
		checkpoint = `,catalog_id='',snapshot_version=-1,refreshed_at_ms=0`
		delay, failures = catalogRefreshInterval, 6
	}
	result, err := tx.ExecContext(ctx, `UPDATE catalog_refresh_state SET lease_token='',eligible_at_ms=$6,failures=$7`+
		checkpoint+` WHERE `+catalogClaimPredicate, append(claim.args(now), now.Add(delay).UnixMilli(), failures)...)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return errCatalogRefreshClaimLost
	}
	return tx.Commit()
}

// Only invalidate the checkpoint that was observed. A newer replacement is
// already usable and must not be rolled back by a slow reader.
func (s catalogSummaryStore) invalidate(ctx context.Context, project string, prior lakeapi.ExportSnapshot, now time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := s.lockActiveProject(ctx, tx, project); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE catalog_refresh_state
SET catalog_id='',snapshot_version=-1,refreshed_at_ms=0,eligible_at_ms=$4,lease_token='',failures=0
WHERE project_id=$1 AND catalog_id=$2 AND snapshot_version=$3 AND catalog_id<>''`, project, prior.CatalogID, prior.Version, now.UnixMilli()); err != nil {
		return err
	}
	return tx.Commit()
}
