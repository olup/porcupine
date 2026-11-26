package acceptance

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

// The journal owns accepted requests until the hosted writer confirms publication.
// Pending requests have a NULL batch ID. Once sealed, the same batch ID and
// request order must reach the writer on every attempt.
type journal struct {
	db     *sql.DB
	unlock func()
}

type journalRequest struct {
	Input
	// Preserve raw JSON bytes across restart, including whitespace.
	Properties, Context, IdentitySet, IdentitySetOnce []byte
}

func (j journal) append(ctx context.Context, project string, owned []BufferedEvent) error {
	request := make([]journalRequest, len(owned))
	for i, event := range owned {
		row := &request[i]
		row.Input = event.Input
		row.Properties, row.Context = row.Event.Properties, row.Event.Context
		row.Event.Properties, row.Event.Context = nil, nil
		if command := row.Identity; command != nil {
			identity := *command
			row.IdentitySet, row.IdentitySetOnce = identity.Set, identity.SetOnce
			identity.Set, identity.SetOnce = nil, nil
			row.Identity = &identity
		}
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return err
	}
	_, err = j.db.ExecContext(ctx, `INSERT INTO requests(project_id, admitted_at, payload) VALUES (?, ?, ?)`,
		project, owned[0].AdmittedAt.UnixNano(), payload)
	return err
}

func (j journal) load(ctx context.Context, b *Buffer) error {
	rows, err := j.db.QueryContext(ctx, `SELECT project_id, batch_id, admitted_at, payload FROM requests ORDER BY seq`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var project string
		var id sql.NullString
		var admittedAt int64
		var payload []byte
		if err := rows.Scan(&project, &id, &admittedAt, &payload); err != nil {
			return err
		}
		var request []journalRequest
		decoder := json.NewDecoder(bytes.NewReader(payload))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&request); err != nil || len(request) == 0 {
			return errors.New("invalid worker journal request")
		}
		p := b.projects[project]
		if p == nil {
			p = &bufferedProject{}
			b.projects[project] = p
			b.bytes += bufferedProjectOverhead + int64(len(project))
		}
		when := time.Unix(0, admittedAt).UTC()
		var batch *FlushBatch
		if id.Valid {
			if n := len(p.ready); n > 0 && p.ready[n-1].ID == id.String {
				batch = p.ready[n-1]
			} else {
				batch = &FlushBatch{ID: id.String, ProjectID: project, oldest: when}
				p.ready = append(p.ready, batch)
			}
		}
		for i, row := range request {
			if row.Event.ProjectID != project {
				return errors.New("worker journal request has an invalid project")
			}
			row.Event.Properties, row.Event.Context = row.Properties, row.Context
			if row.Identity != nil {
				row.Identity.Set, row.Identity.SetOnce = row.IdentitySet, row.IdentitySetOnce
			}
			event := BufferedEvent{Input: row.Input, AdmittedAt: when, requestEnd: i == len(request)-1}
			size := bufferedInputBytes(event.Input)
			if batch != nil {
				batch.Events = append(batch.Events, event)
				batch.bytes += size
			} else {
				if len(p.pending) == 0 {
					p.oldest = when
				}
				p.pending = append(p.pending, event)
				p.pendingBytes += size
			}
			b.events++
			b.bytes += size
		}
	}
	return rows.Err()
}

func (j journal) seal(ctx context.Context, batch *FlushBatch) error {
	_, err := j.db.ExecContext(ctx, `UPDATE requests SET batch_id=? WHERE project_id=? AND batch_id IS NULL`, batch.ID, batch.ProjectID)
	return err
}

// Split both children in one transaction before letting either reach DuckLake.
func (j journal) split(ctx context.Context, parent string, left, right *FlushBatch) error {
	count := 0
	for _, event := range left.Events {
		if event.requestEnd {
			count++
		}
	}
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `UPDATE requests SET batch_id=?
		WHERE seq IN (SELECT seq FROM requests WHERE project_id=? AND batch_id=? ORDER BY seq LIMIT ?)`,
		left.ID, left.ProjectID, parent, count); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE requests SET batch_id=? WHERE project_id=? AND batch_id=?`,
		right.ID, right.ProjectID, parent); err != nil {
		return err
	}
	return tx.Commit()
}

func (j journal) remove(batch *FlushBatch, fenced bool) error {
	// A writer can confirm publication just as the worker is canceled.
	// Keep that acknowledgment even during shutdown.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if fenced {
		_, err := j.db.ExecContext(ctx, `DELETE FROM requests WHERE project_id=?`, batch.ProjectID)
		return err
	}
	_, err := j.db.ExecContext(ctx, `DELETE FROM requests WHERE project_id=? AND batch_id=?`, batch.ProjectID, batch.ID)
	return err
}
