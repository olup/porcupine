package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/olup/porcupine/internal/apierr"
	"github.com/olup/porcupine/internal/event"
	"github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/nativequery"
)

const errorIssueSchema = `CREATE TABLE error_issues (
	project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
	id TEXT NOT NULL,
	status TEXT NOT NULL CHECK(status IN ('open','acknowledged','resolved','seen_again')),
	muted BIGINT NOT NULL CHECK(muted IN (0,1)),
	revision BIGINT NOT NULL CHECK(revision > 0),
	updated_at TEXT NOT NULL,
	resolved_at TEXT NOT NULL DEFAULT '',
	seen_again_at TEXT NOT NULL DEFAULT '',
	seen_again_event_id TEXT NOT NULL DEFAULT '',
	updated_by TEXT NOT NULL,
	PRIMARY KEY(project_id,id)
);
CREATE TABLE error_issue_fingerprints (
	project_id TEXT NOT NULL,
	fingerprint TEXT NOT NULL,
	issue_id TEXT NOT NULL,
	linked_at TEXT NOT NULL,
	PRIMARY KEY(project_id,fingerprint),
	FOREIGN KEY(project_id,issue_id) REFERENCES error_issues(project_id,id) ON DELETE CASCADE
);
CREATE INDEX error_issue_fingerprints_by_issue ON error_issue_fingerprints(project_id,issue_id);
CREATE INDEX error_issues_muted ON error_issues(project_id,id) WHERE muted=1;`

const maxErrorIssueFingerprints = 32

var errorIssueID = regexp.MustCompile(`^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$`)

type ErrorIssue struct {
	ID               string `json:"id,omitempty" doc:"Stable project issue ID. Present when the fingerprint is attached to a tracked issue."`
	Status           string `json:"status" enum:"open,acknowledged,resolved,seen_again"`
	Muted            bool   `json:"muted" doc:"Mute is independent of the workflow status."`
	Revision         int64  `json:"revision" doc:"Use for optimistic concurrency. Zero denotes an untracked fingerprint."`
	UpdatedAt        string `json:"updated_at,omitempty" format:"date-time"`
	ResolvedAt       string `json:"resolved_at,omitempty" format:"date-time"`
	SeenAgainAt      string `json:"seen_again_at,omitempty" format:"date-time" doc:"Occurrence time of the delivered exception that caused Seen again."`
	SeenAgainEventID string `json:"seen_again_event_id,omitempty" doc:"Decimal analytics event ID of that exception."`
}

type ErrorIssueInput struct {
	ID      string `path:"id" doc:"Project identifier"`
	IssueID string `path:"issue_id" doc:"Stable error issue ID."`
}

type ErrorGroupTriageInput struct {
	ID          string `path:"id" doc:"Project identifier"`
	Fingerprint string `path:"fingerprint" doc:"Lowercase hexadecimal exception fingerprint."`
	Body        struct {
		Action           string `json:"action" enum:"acknowledge,mute,unmute,resolve,reopen,link,unlink"`
		ExpectedRevision int64  `json:"expected_revision" minimum:"0" doc:"Revision of the current issue, or zero for an untracked fingerprint. For link, use the target issue revision."`
		IssueID          string `json:"issue_id,omitempty" doc:"Target stable issue ID, required for link."`
	}
}

type ErrorIssueOutput struct {
	Body struct {
		Issue        ErrorIssue `json:"issue"`
		Fingerprints []string   `json:"fingerprints" doc:"Attached build fingerprints across all occurrence windows."`
	}
}

type errorIssueRecord struct {
	ErrorIssue
	links map[string]string
}

func (issue *errorIssueRecord) output() *ErrorIssueOutput {
	output := &ErrorIssueOutput{}
	output.Body.Issue = issue.ErrorIssue
	output.Body.Fingerprints = make([]string, 0, len(issue.links))
	for fingerprint := range issue.links {
		output.Body.Fingerprints = append(output.Body.Fingerprints, fingerprint)
	}
	slices.Sort(output.Body.Fingerprints)
	return output
}

func errorIssueUnavailable() error {
	return apierr.New(503, "error issue storage unavailable")
}

func errorIssueConflict() error {
	return apierr.New(409, "error issue changed; read the current issue and retry")
}

func readErrorIssues(ctx context.Context, db oauthQueryer, project, issueID string, fingerprints []string) ([]*errorIssueRecord, error) {
	params := []any{project}
	selection := "i.id=$2"
	if issueID != "" {
		params = append(params, issueID)
	} else {
		if len(fingerprints) == 0 {
			return nil, nil
		}
		placeholders := make([]string, 0, len(fingerprints))
		for _, fingerprint := range fingerprints {
			params = append(params, fingerprint)
			placeholders = append(placeholders, fmt.Sprintf("$%d", len(params)))
		}
		selection = `i.id IN (SELECT issue_id FROM error_issue_fingerprints WHERE project_id=$1 AND fingerprint IN (` +
			strings.Join(placeholders, ",") + "))"
	}
	rows, err := db.QueryContext(ctx, `SELECT i.id,i.status,i.muted,i.revision,i.updated_at,i.resolved_at,i.seen_again_at,i.seen_again_event_id,
f.fingerprint,f.linked_at FROM error_issues i JOIN error_issue_fingerprints f ON f.project_id=i.project_id AND f.issue_id=i.id
WHERE i.project_id=$1 AND `+selection+` ORDER BY i.id,f.fingerprint`, params...)
	if err != nil {
		return nil, errorIssueUnavailable()
	}
	defer rows.Close()
	var issues []*errorIssueRecord
	var last *errorIssueRecord
	for rows.Next() {
		var issue ErrorIssue
		var fingerprint, linkedAt string
		if err := rows.Scan(&issue.ID, &issue.Status, &issue.Muted, &issue.Revision, &issue.UpdatedAt, &issue.ResolvedAt,
			&issue.SeenAgainAt, &issue.SeenAgainEventID, &fingerprint, &linkedAt); err != nil {
			return nil, errorIssueUnavailable()
		}
		if last == nil || last.ID != issue.ID {
			last = &errorIssueRecord{ErrorIssue: issue, links: make(map[string]string)}
			issues = append(issues, last)
		}
		last.links[fingerprint] = linkedAt
	}
	if rows.Err() != nil {
		return nil, errorIssueUnavailable()
	}
	return issues, nil
}

func (s *Service) getErrorIssue(ctx context.Context, input *ErrorIssueInput) (*ErrorIssueOutput, error) {
	if !errorIssueID.MatchString(input.IssueID) {
		return nil, apierr.New(400, "invalid error issue ID")
	}
	if err := s.analyticsProject(ctx, input.ID); err != nil {
		return nil, err
	}
	issues, err := readErrorIssues(ctx, s.control, input.ID, input.IssueID, nil)
	if err != nil {
		return nil, err
	}
	if len(issues) == 0 {
		return nil, apierr.New(404, "error issue not found")
	}
	s.refreshErrorIssueRecurrence(ctx, input.ID, issues)
	return issues[0].output(), nil
}

func (s *Service) mutedErrorGroupFingerprints(ctx context.Context, project string) (string, error) {
	const maxFingerprints = (nativequery.MaxInternalParameterBytes - 2) / 69
	rows, err := s.control.QueryContext(ctx, `SELECT f.fingerprint FROM error_issues i
JOIN error_issue_fingerprints f ON f.project_id=i.project_id AND f.issue_id=i.id
WHERE i.project_id=$1 AND i.muted=1 LIMIT $2`, project, maxFingerprints+1)
	if err != nil {
		return "", errorIssueUnavailable()
	}
	defer rows.Close()
	muted := make(map[string]int)
	for rows.Next() {
		var fingerprint string
		if err := rows.Scan(&fingerprint); err != nil {
			return "", errorIssueUnavailable()
		}
		if len(muted) == maxFingerprints {
			return "", apierr.New(429, "muted error fingerprint filter limit reached")
		}
		muted[fingerprint] = 1
	}
	if rows.Err() != nil {
		return "", errorIssueUnavailable()
	}
	data, err := json.Marshal(muted)
	if err != nil {
		return "", errorIssueUnavailable()
	}
	return string(data), nil
}

func (s *Service) annotateErrorGroups(ctx context.Context, project string, groups []ErrorGroupSummary) error {
	fingerprints := make([]string, 0, len(groups))
	for _, group := range groups {
		fingerprints = append(fingerprints, group.Fingerprint)
	}
	issues, err := readErrorIssues(ctx, s.control, project, "", fingerprints)
	if err != nil {
		return err
	}
	s.refreshErrorIssueRecurrence(ctx, project, issues)
	byFingerprint := make(map[string]ErrorIssue)
	for _, issue := range issues {
		for fingerprint := range issue.links {
			byFingerprint[fingerprint] = issue.ErrorIssue
		}
	}
	for index := range groups {
		groups[index].Issue = byFingerprint[groups[index].Fingerprint]
		if groups[index].Issue.ID == "" {
			groups[index].Issue.Status = "open"
		}
	}
	return nil
}

type errorIssueWatch struct {
	IssueID string `json:"issue_id"`
	Cutoff  string `json:"cutoff"`
}

type errorIssueEvidence struct {
	IssueID    string `json:"issue_id"`
	EventID    string `json:"event_id"`
	OccurredAt string `json:"occurred_at"`
}

func (s *Service) refreshErrorIssueRecurrence(ctx context.Context, project string, issues []*errorIssueRecord) {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := s.observeErrorIssues(ctx, project, issues); err != nil {
		slog.DebugContext(ctx, "error issue recurrence refresh failed", "project_id", project, "error", err)
	}
}

func (s *Service) observeErrorIssues(ctx context.Context, project string, issues []*errorIssueRecord) error {
	watches := make(map[string]errorIssueWatch)
	var earliest time.Time
	for _, issue := range issues {
		if issue.Status != "resolved" {
			continue
		}
		resolved, err := time.Parse(time.RFC3339Nano, issue.ResolvedAt)
		if err != nil {
			return errorIssueUnavailable()
		}
		for fingerprint, linkedAt := range issue.links {
			linked, err := time.Parse(time.RFC3339Nano, linkedAt)
			if err != nil {
				return errorIssueUnavailable()
			}
			cutoff := resolved
			if linked.After(cutoff) {
				cutoff = linked
			}
			if earliest.IsZero() || cutoff.Before(earliest) {
				earliest = cutoff
			}
			watches[fingerprint] = errorIssueWatch{IssueID: issue.ID, Cutoff: cutoff.UTC().Format("2006-01-02T15:04:05000000000")}
		}
	}
	asOf := s.clock().UTC()
	if len(watches) == 0 || !earliest.Before(asOf) {
		return nil
	}
	raw, err := json.Marshal(watches)
	if err != nil {
		return errorIssueUnavailable()
	}
	window := &lake.QueryTimeRange{From: earliest, To: asOf}
	data, _, err := s.readErrorGroupsWithInternalParameter(ctx, project, errorIssueEvidenceSQL, map[string]string{"watches": string(raw)}, window, "watches")
	if err != nil {
		return err
	}
	evidence := make(map[string]errorIssueEvidence, len(data))
	for _, row := range data {
		var seen errorIssueEvidence
		if err := decodeErrorGroupRow(row, &seen); err != nil {
			return err
		}
		evidence[seen.IssueID] = seen
	}
	for _, issue := range issues {
		seen, ok := evidence[issue.ID]
		if !ok {
			continue
		}
		updatedAt := s.clock().UTC().Format(time.RFC3339Nano)
		result, err := s.control.ExecContext(ctx, `UPDATE error_issues SET status='seen_again', revision=revision+1,
updated_at=$4, seen_again_at=$5, seen_again_event_id=$6, updated_by=''
WHERE project_id=$1 AND id=$2 AND revision=$3 AND status='resolved'
AND NOT EXISTS(SELECT 1 FROM project_deletions WHERE project_id=$1)
AND NOT EXISTS(SELECT 1 FROM projects p JOIN tenant_deletions d ON d.tenant_id=p.tenant_id WHERE p.id=$1)`,
			project, issue.ID, issue.Revision, updatedAt, seen.OccurredAt, seen.EventID)
		if err != nil {
			return errorIssueUnavailable()
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return errorIssueUnavailable()
		}
		if changed == 1 {
			issue.Status = "seen_again"
			issue.Revision++
			issue.UpdatedAt = updatedAt
			issue.SeenAgainAt = seen.OccurredAt
			issue.SeenAgainEventID = seen.EventID
		}
	}
	return nil
}

func (s *Service) lockErrorIssueProject(ctx context.Context, tx *sql.Tx, project string) error {
	if s.shared != nil {
		// Use deletion's tenant then project lock order, without upgrading a
		// shared project lock while another triage request is also holding it.
		var tenant, locked string
		err := tx.QueryRowContext(ctx, `SELECT t.id FROM tenants t JOIN projects p ON p.tenant_id=t.id WHERE p.id=$1 FOR SHARE OF t`, project).Scan(&tenant)
		if err == nil {
			err = tx.QueryRowContext(ctx, `SELECT id FROM projects WHERE id=$1 AND tenant_id=$2 FOR NO KEY UPDATE`, project, tenant).Scan(&locked)
		}
		if errors.Is(err, sql.ErrNoRows) {
			return apierr.New(404, "project not found")
		}
		if err != nil {
			return errorIssueUnavailable()
		}
	} else if _, err := tx.ExecContext(ctx, `UPDATE projects SET id=id WHERE id=$1`, project); err != nil {
		// On SQLite a write acquires the write lock before reading the revision.
		return errorIssueUnavailable()
	}
	return requireActiveProjectSet(ctx, tx, []string{project})
}

func (s *Service) triageErrorGroup(ctx context.Context, input *ErrorGroupTriageInput) (*ErrorIssueOutput, error) {
	if !errorGroupFingerprint.MatchString(input.Fingerprint) {
		return nil, apierr.New(400, "fingerprint must be 64 lowercase hexadecimal characters")
	}
	project, fingerprint := input.ID, input.Fingerprint
	action, target := input.Body.Action, input.Body.IssueID
	if action == "link" && !errorIssueID.MatchString(target) || action != "link" && target != "" {
		return nil, apierr.New(400, "use a valid issue_id only when linking a fingerprint")
	}
	issues, err := readErrorIssues(ctx, s.control, project, "", []string{fingerprint})
	if err != nil {
		return nil, err
	}
	if len(issues) == 0 {
		if action == "unlink" || action != "link" && input.Body.ExpectedRevision != 0 {
			return nil, errorIssueConflict()
		}
		// Track only fingerprints that exist in the project's delivered analytics.
		data, _, err := s.readErrorGroups(ctx, project, errorIssueFingerprintSQL, map[string]string{"fingerprint": fingerprint}, nil)
		if err != nil {
			return nil, err
		}
		if len(data) == 0 {
			return nil, apierr.New(404, "delivered error group not found")
		}
	}
	if action == "link" {
		issues, err = readErrorIssues(ctx, s.control, project, target, nil)
		if err != nil {
			return nil, err
		}
	}

	tx, err := s.control.BeginTx(ctx, nil)
	if err != nil {
		return nil, errorIssueUnavailable()
	}
	defer tx.Rollback()
	if err := s.lockErrorIssueProject(ctx, tx, project); err != nil {
		return nil, err
	}
	if err := s.requireExternalCurrent(ctx, tx); err != nil {
		return nil, err
	}
	current, err := readErrorIssues(ctx, tx, project, "", []string{fingerprint})
	if err != nil {
		return nil, err
	}
	if action == "link" && len(current) != 0 {
		return nil, errorIssueConflict()
	}
	if action == "link" {
		current, err = readErrorIssues(ctx, tx, project, target, nil)
		if err != nil {
			return nil, err
		}
		if len(current) == 0 {
			return nil, apierr.New(404, "error issue not found")
		}
	}

	var issue *errorIssueRecord
	if len(current) == 0 {
		// A concurrent unlink removed the mapping after the existence check.
		if len(issues) > 0 || input.Body.ExpectedRevision != 0 || action == "unlink" {
			return nil, errorIssueConflict()
		}
		issue = &errorIssueRecord{ErrorIssue: ErrorIssue{Status: "open"}, links: map[string]string{}}
	} else {
		issue = current[0]
		if issue.Revision != input.Body.ExpectedRevision || len(issues) == 0 || issue.ID != issues[0].ID {
			return nil, errorIssueConflict()
		}
	}
	at := s.clock().UTC().Format(time.RFC3339Nano)
	before := issue.ErrorIssue
	switch action {
	case "link":
		if len(issue.links) >= maxErrorIssueFingerprints {
			return nil, apierr.New(409, "error issue fingerprint limit reached")
		}
		issue.links[fingerprint] = at
	case "unlink":
		if len(issue.links) == 1 {
			return nil, apierr.New(409, "cannot unlink the last fingerprint from an error issue")
		}
		delete(issue.links, fingerprint)
	case "acknowledge":
		if issue.Status == "resolved" {
			return nil, apierr.New(409, "reopen the resolved issue before acknowledging it")
		}
		issue.Status = "acknowledged"
	case "mute":
		issue.Muted = true
	case "unmute":
		issue.Muted = false
	case "resolve":
		if issue.Status != "resolved" {
			issue.Status = "resolved"
			issue.ResolvedAt = at
			issue.SeenAgainAt = ""
			issue.SeenAgainEventID = ""
		}
	case "reopen":
		issue.Status = "open"
	default:
		return nil, apierr.New(400, "unknown error triage action")
	}
	created := issue.Revision == 0
	if action != "link" && action != "unlink" && issue.ErrorIssue == before {
		if created {
			issue.links[fingerprint] = ""
		}
		return issue.output(), nil
	}
	if created {
		issue.ID = event.NewID()
		if _, err := tx.ExecContext(ctx, `INSERT INTO error_issues(project_id,id,status,muted,revision,updated_at,resolved_at,updated_by)
VALUES ($1,$2,$3,$4,1,$5,$6,$7)`, project, issue.ID, issue.Status, issue.Muted, at, issue.ResolvedAt, principal(ctx).ID); err != nil {
			return nil, errorIssueUnavailable()
		}
		issue.links[fingerprint] = at
	} else {
		result, err := tx.ExecContext(ctx, `UPDATE error_issues SET status=$4,muted=$5,revision=revision+1,updated_at=$6,resolved_at=$7,
seen_again_at=$8,seen_again_event_id=$9,updated_by=$10 WHERE project_id=$1 AND id=$2 AND revision=$3`,
			project, issue.ID, issue.Revision, issue.Status, issue.Muted, at, issue.ResolvedAt,
			issue.SeenAgainAt, issue.SeenAgainEventID, principal(ctx).ID)
		if err != nil {
			return nil, errorIssueUnavailable()
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return nil, errorIssueUnavailable()
		}
		if changed != 1 {
			return nil, errorIssueConflict()
		}
	}
	switch {
	case created || action == "link":
		_, err = tx.ExecContext(ctx, `INSERT INTO error_issue_fingerprints(project_id,fingerprint,issue_id,linked_at) VALUES ($1,$2,$3,$4)`,
			project, fingerprint, issue.ID, at)
	case action == "unlink":
		_, err = tx.ExecContext(ctx, `DELETE FROM error_issue_fingerprints WHERE project_id=$1 AND fingerprint=$2`, project, fingerprint)
	}
	if err != nil || tx.Commit() != nil {
		return nil, errorIssueUnavailable()
	}
	issue.Revision++
	issue.UpdatedAt = at
	return issue.output(), nil
}
