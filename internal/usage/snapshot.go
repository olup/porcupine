package usage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type snapshotCursor struct {
	ID    string `json:"id"`
	Page  int    `json:"page"`
	Hash  string `json:"hash"`
	Epoch string `json:"epoch"`
}
type snapshotMeta struct {
	id, epoch, hash, query, created, expires string
	pages, limit                             int
}

func normalizeQuery(q Query, now time.Time, retention int) (Query, error) {
	q.Cursor = ""
	if q.Granularity == "" {
		q.Granularity = "day"
	}
	if q.GroupBy == "" {
		q.GroupBy = "project"
	}
	if q.Limit == 0 {
		q.Limit = PageDefault
	}
	if (q.Granularity != "day" && q.Granularity != "month") || (q.GroupBy != "project" && q.GroupBy != "tenant") || q.Limit < 1 || q.Limit > PageMax || len(q.TenantID) > 128 {
		return q, ErrCursor
	}
	if q.Start == "" {
		q.Start = time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339)
	}
	start, e := time.Parse(time.RFC3339, q.Start)
	if e != nil || Day(start) != q.Start {
		return q, fmt.Errorf("%w: start must be a UTC midnight", ErrQuery)
	}
	if q.End == "" {
		q.End = Day(now.AddDate(0, 0, 1))
	}
	end, e := time.Parse(time.RFC3339, q.End)
	if e != nil || Day(end) != q.End || !end.After(start) || end.Sub(start) > MaxPeriod {
		return q, fmt.Errorf("%w: end must be a later UTC midnight, at most 366 days after start", ErrQuery)
	}
	if q.Start < Day(now.AddDate(0, 0, -retention)) {
		return q, fmt.Errorf("%w: period exceeds retained history", ErrQuery)
	}
	return q, nil
}
func queryIdentity(q Query) (string, string) {
	b, _ := json.Marshal(q)
	h := sha256.Sum256(b)
	return string(b), hex.EncodeToString(h[:])
}
func cursorFor(m snapshotMeta, page int) string {
	b, _ := json.Marshal(snapshotCursor{m.id, page, m.hash, m.epoch})
	return base64.RawURLEncoding.EncodeToString(b)
}
func (s *Store) ReadSnapshot(ctx context.Context, q Query) (UsagePage, error) {
	if q.Cursor != "" {
		return s.readPage(ctx, q)
	}
	q, err := normalizeQuery(q, s.Clock().UTC(), s.Config.RetentionDays)
	if err != nil {
		return UsagePage{}, err
	}
	select {
	case s.localBuild <- struct{}{}:
		defer func() { <-s.localBuild }()
	default:
		return UsagePage{}, ErrCapacity
	}
	// The reservation precedes every potentially large read or allocation. Its
	// expiry recovers a crashed builder; final publication is fenced by its token.
	token, budget, epoch, err := s.reserveBuild(ctx)
	if err != nil {
		return UsagePage{}, err
	}
	defer s.releaseBuild(context.WithoutCancel(ctx), token)
	build, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	rows, err := s.materialize(build, q, budget)
	if err != nil {
		return UsagePage{}, err
	}
	created := s.Clock()
	query, hash := queryIdentity(q)
	m := snapshotMeta{id: NewID(), epoch: epoch, hash: hash, query: query, created: Stamp(created), expires: Stamp(created.Add(SnapshotTTL)), limit: q.Limit}
	pages := []string{}
	var size int64
	for start := 0; start < len(rows) || start == 0; start += q.Limit {
		end := min(start+q.Limit, len(rows))
		b, e := json.Marshal(rows[start:end])
		if e != nil {
			return UsagePage{}, e
		}
		size += int64(len(b))
		if size > budget {
			return UsagePage{}, ErrTooLarge
		}
		pages = append(pages, string(b))
		if end == len(rows) {
			break
		}
	}
	m.pages = len(pages)
	if err = s.saveSnapshot(build, m, q.Start, pages, size, token); err != nil {
		return UsagePage{}, err
	}
	return pageFrom(m, 0, rows[:min(len(rows), q.Limit)]), nil
}
func pageFrom(m snapshotMeta, index int, rows []UsageRow) UsagePage {
	p := UsagePage{MetricVersion: Version, Epoch: m.epoch, SnapshotID: m.id, CreatedAt: m.created, ExpiresAt: m.expires, PublicationStatus: "unknown", Rows: rows}
	if index+1 < m.pages {
		p.NextCursor = cursorFor(m, index+1)
	}
	return p
}
func (s *Store) lockEpoch(ctx context.Context, tx *sql.Tx) (string, error) {
	if !s.Postgres {
		if _, e := tx.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='usage_epoch'`); e != nil {
			return "", e
		}
	}
	q := `SELECT value FROM metadata WHERE key='usage_epoch'`
	if s.Postgres {
		q += ` FOR UPDATE`
	}
	var epoch string
	e := tx.QueryRowContext(ctx, q).Scan(&epoch)
	return epoch, e
}
func (s *Store) reserveBuild(ctx context.Context) (string, int64, string, error) {
	tx, e := s.DB.BeginTx(ctx, nil)
	if e != nil {
		return "", 0, "", e
	}
	defer tx.Rollback()
	epoch, e := s.lockEpoch(ctx, tx)
	if e != nil {
		return "", 0, "", e
	}
	now := Stamp(s.Clock())
	if _, e = tx.ExecContext(ctx, `DELETE FROM usage_snapshots WHERE expires_at<=$1`, now); e != nil {
		return "", 0, "", e
	}
	if _, e = tx.ExecContext(ctx, `DELETE FROM usage_snapshot_build WHERE expires_at<=$1`, now); e != nil {
		return "", 0, "", e
	}
	var builders, count int
	var bytes int64
	if e = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM usage_snapshot_build`).Scan(&builders); e != nil {
		return "", 0, "", e
	}
	if e = tx.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(total_bytes),0) FROM usage_snapshots`).Scan(&count, &bytes); e != nil {
		return "", 0, "", e
	}
	if builders != 0 || count >= s.Config.MaxSnapshots || bytes >= s.Config.MaxSnapshotBytes {
		return "", 0, "", ErrCapacity
	}
	budget := min(int64(MaxBytes), s.Config.MaxSnapshotBytes-bytes)
	token := NewID()
	if _, e = tx.ExecContext(ctx, `INSERT INTO usage_snapshot_build(id,token,reserved_bytes,expires_at) VALUES('global',$1,$2,$3)`, token, budget, Stamp(s.Clock().Add(time.Minute))); e != nil {
		return "", 0, "", e
	}
	if e = tx.Commit(); e != nil {
		return "", 0, "", e
	}
	return token, budget, epoch, nil
}
func (s *Store) releaseBuild(ctx context.Context, token string) {
	bounded, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, _ = s.DB.ExecContext(bounded, `DELETE FROM usage_snapshot_build WHERE id='global' AND token=$1`, token)
}
func (s *Store) saveSnapshot(ctx context.Context, m snapshotMeta, start string, pages []string, size int64, token string) error {
	tx, e := s.DB.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	epoch, e := s.lockEpoch(ctx, tx)
	if e != nil {
		return e
	}
	if epoch != m.epoch {
		return ErrExpired
	}
	var reserved int64
	var expiry string
	if e = tx.QueryRowContext(ctx, `SELECT reserved_bytes,expires_at FROM usage_snapshot_build WHERE id='global' AND token=$1`, token).Scan(&reserved, &expiry); e != nil {
		return ErrCapacity
	}
	if size > reserved || expiry <= Stamp(s.Clock()) {
		return ErrCapacity
	}
	if _, e = tx.ExecContext(ctx, `INSERT INTO usage_snapshots(id,epoch,query_hash,query_json,created_at,expires_at,period_start,total_bytes,page_count,limit_rows) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, m.id, m.epoch, m.hash, m.query, m.created, m.expires, start, size, m.pages, m.limit); e != nil {
		return e
	}
	for i, p := range pages {
		if _, e = tx.ExecContext(ctx, `INSERT INTO usage_snapshot_pages(snapshot_id,page_index,payload) VALUES($1,$2,$3)`, m.id, i, p); e != nil {
			return e
		}
	}
	if _, e = tx.ExecContext(ctx, `DELETE FROM usage_snapshot_build WHERE id='global' AND token=$1`, token); e != nil {
		return e
	}
	return tx.Commit()
}
func (s *Store) readPage(ctx context.Context, q Query) (UsagePage, error) {
	if len(q.Cursor) > 2048 {
		return UsagePage{}, ErrCursor
	}
	var c snapshotCursor
	b, e := base64.RawURLEncoding.DecodeString(q.Cursor)
	if e != nil || len(b) > 1024 {
		return UsagePage{}, ErrCursor
	}
	if json.Unmarshal(b, &c) != nil || c.ID == "" || c.Page < 1 {
		return UsagePage{}, ErrCursor
	}
	var m snapshotMeta
	var payload, epoch string
	e = s.DB.QueryRowContext(ctx, `SELECT s.id,s.epoch,s.query_hash,s.query_json,s.created_at,s.expires_at,s.page_count,s.limit_rows,p.payload,(SELECT value FROM metadata WHERE key='usage_epoch') FROM usage_snapshots s JOIN usage_snapshot_pages p ON p.snapshot_id=s.id AND p.page_index=$2 WHERE s.id=$1`, c.ID, c.Page).Scan(&m.id, &m.epoch, &m.hash, &m.query, &m.created, &m.expires, &m.pages, &m.limit, &payload, &epoch)
	if errors.Is(e, sql.ErrNoRows) {
		return UsagePage{}, ErrExpired
	}
	if e != nil {
		return UsagePage{}, e
	}
	if m.expires <= Stamp(s.Clock()) || m.epoch != epoch || m.epoch != c.Epoch {
		return UsagePage{}, ErrExpired
	}
	if m.hash != c.Hash {
		return UsagePage{}, ErrCursor
	}
	var original Query
	if json.Unmarshal([]byte(m.query), &original) != nil {
		return UsagePage{}, ErrCursor
	}
	if (q.Start != "" && q.Start != original.Start) || (q.End != "" && q.End != original.End) || (q.TenantID != "" && q.TenantID != original.TenantID) || (q.GroupBy != "" && q.GroupBy != original.GroupBy) || (q.Granularity != "" && q.Granularity != original.Granularity) || (q.Limit != 0 && q.Limit != m.limit) {
		return UsagePage{}, ErrCursor
	}
	var rows []UsageRow
	if e = json.Unmarshal([]byte(payload), &rows); e != nil {
		return UsagePage{}, e
	}
	return pageFrom(m, c.Page, rows), nil
}
