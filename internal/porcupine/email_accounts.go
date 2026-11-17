package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/danielgtaylor/huma/v2"
	"slices"
	"time"

	"github.com/olup/porcupine/internal/auth"
)

type HumanProjectGrant struct {
	ProjectID   string   `json:"project_id" minLength:"1" maxLength:"256"`
	Permissions []string `json:"permissions" minItems:"1" maxItems:"32"`
}

type EmailHuman struct {
	Grants          []HumanProjectGrant `json:"grants" doc:"Effective permissions for each project."`
	ID              string              `json:"id"`
	Email           string              `json:"email"`
	Projects        []string            `json:"projects"`
	Permissions     []string            `json:"permissions"`
	DeploymentAdmin bool                `json:"deployment_admin"`
	CreatedAt       string              `json:"created_at"`
	RevokedAt       *string             `json:"revoked_at" nullable:"true"`
}

type InviteHumanInput struct {
	Body struct {
		Grants      []HumanProjectGrant `json:"grants,omitempty" minItems:"1" maxItems:"64" doc:"Per-project rights. Alternatively supply both projects and permissions."`
		Email       string              `json:"email" minLength:"3" maxLength:"254"`
		Projects    []string            `json:"projects,omitempty" minItems:"1" maxItems:"64" doc:"Explicit project IDs. For an instance administrator, use only * with only instance:admin."`
		Permissions []string            `json:"permissions,omitempty" minItems:"1" maxItems:"32" doc:"Permissions applied in every assigned project."`
	}
}
type EmailHumanOutput struct{ Body EmailHuman }
type UpdateHumanInput struct {
	ID   string `path:"id"`
	Body struct {
		Grants      []HumanProjectGrant `json:"grants,omitempty" minItems:"1" maxItems:"64"`
		Projects    []string            `json:"projects,omitempty" minItems:"1" maxItems:"64"`
		Permissions []string            `json:"permissions,omitempty" minItems:"1" maxItems:"32"`
	}
}
type HumanIDInput struct {
	ID string `path:"id"`
}
type ListHumansInput struct {
	Limit  int `query:"limit" default:"20" minimum:"1" maximum:"100"`
	Offset int `query:"offset" default:"0" minimum:"0"`
}
type ListHumansOutput struct {
	Body struct {
		Humans     []EmailHuman `json:"humans"`
		NextOffset *int         `json:"next_offset,omitempty"`
	}
}

// Use the SMTP mailbox as the identity. DNS names are case-insensitive, while
// a relay is allowed to distinguish the case of local parts.
func (s *Service) initializeEmailAccounts(ctx context.Context) error {
	if s.oauth == nil || s.oauth.config.Email == nil || s.oauth.config.Email.AdminEmail == "" {
		return nil
	}
	if blocked, err := s.restoredIdentityBootstrapBlocked(ctx); err != nil || blocked {
		return err
	}
	address, err := auth.NormalizeEmail(s.oauth.config.Email.AdminEmail)
	if err != nil {
		return err
	}
	tx, err := s.control.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Take the SQLite writer lock before checking the existing deployment pin.
	if _, err := tx.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		return err
	}
	var pinned string
	err = tx.QueryRowContext(ctx, `SELECT email FROM email_humans WHERE deployment_admin=1`).Scan(&pinned)
	if err == nil {
		if pinned != address {
			return errors.New("deployment admin email differs from the provisioned email")
		}
		return tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var exists bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM email_humans WHERE email=$1)`, address).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return errors.New("deployment admin email already belongs to an invited person")
	}
	admin := EmailHuman{
		ID: newID(), Email: address, CreatedAt: now(), DeploymentAdmin: true,
		Permissions: []string{"instance:admin"}, Projects: []string{"*"},
		Grants: []HumanProjectGrant{{ProjectID: "*", Permissions: []string{"instance:admin"}}},
	}
	if err := insertEmailHuman(ctx, tx, admin); err != nil {
		return err
	}
	return tx.Commit()
}

func insertEmailHuman(ctx context.Context, tx *sql.Tx, human EmailHuman) error {
	permissionsJSON, _ := json.Marshal(human.Permissions)
	projectsJSON, _ := json.Marshal(human.Projects)
	_, err := tx.ExecContext(ctx, `INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at) VALUES($1,'Email human',$2,$3,$4,$5)`,
		human.ID, hash(token()), string(permissionsJSON), string(projectsJSON), human.CreatedAt)
	if err != nil {
		return err
	}
	grants, _ := json.Marshal(human.Grants)
	_, err = tx.ExecContext(ctx, `INSERT INTO email_humans(principal_id,email,deployment_admin,project_grants) VALUES($1,$2,$3,$4)`, human.ID, human.Email, human.DeploymentAdmin, string(grants))
	return err
}

func loadEmailHuman(ctx context.Context, query oauthQueryer, id string) (auth.Principal, error) {
	var permissionsJSON, projectsJSON, grantsJSON string
	err := query.QueryRowContext(ctx, `SELECT k.permissions,k.projects,h.project_grants FROM email_humans h JOIN access_keys k ON k.id=h.principal_id WHERE k.id=$1 AND k.revoked_at IS NULL`, id).Scan(&permissionsJSON, &projectsJSON, &grantsJSON)
	if err != nil {
		return auth.Principal{}, err
	}
	a := auth.Principal{ID: id, EmailHuman: true}
	if json.Unmarshal([]byte(permissionsJSON), &a.Permissions) != nil || json.Unmarshal([]byte(projectsJSON), &a.Projects) != nil {
		return auth.Principal{}, errors.New("invalid email account grants")
	}
	var grants []HumanProjectGrant
	if err := json.Unmarshal([]byte(grantsJSON), &grants); err != nil {
		return auth.Principal{}, errors.New("invalid email project grants")
	}
	a.ProjectPermissions = make(map[string][]string, len(grants))
	for _, g := range grants {
		a.ProjectPermissions[g.ProjectID] = g.Permissions
	}
	return addTenantMembershipAuthority(ctx, query, a)
}

func (s *Service) lookupActiveEmailID(ctx context.Context, email string) (string, error) {
	var id string
	address, err := auth.NormalizeEmail(email)
	if err != nil {
		return "", err
	}
	err = s.control.QueryRowContext(ctx, `SELECT k.id FROM email_humans h JOIN access_keys k ON k.id=h.principal_id WHERE h.email=$1 AND k.revoked_at IS NULL`, address).Scan(&id)
	return id, err
}

func (s *Service) registerEmailAccounts(api huma.API, op func(string, string, string, string, auth.OperationAccess, int) huma.Operation) {
	if s.oauth == nil || s.oauth.config.Email == nil {
		return
	}
	admin := auth.OperationAccess{Permission: "instance:admin"}
	base := "/api/v1/humans"
	huma.Register(api, op("invite-human", "POST", base, "Invite a project member or instance administrator. For an administrator use projects [*] and permissions [instance:admin].", admin, 201), s.inviteHuman)
	huma.Register(api, op("list-humans", "GET", base, "List human accounts and project permissions", admin, 200), s.listHumans)
	huma.Register(api, op("update-human", "PUT", base+"/{id}", "Replace an invited human’s grants and invalidate affected connections", admin, 200), s.updateHuman)
	huma.Register(api, op("revoke-human", "DELETE", base+"/{id}", "Disable this account and its OAuth connections", admin, 204), s.revokeHuman)
}

const selectEmailHumans = `SELECT k.id,h.email,h.deployment_admin,k.permissions,k.projects,k.created_at,k.revoked_at,h.project_grants FROM email_humans h JOIN access_keys k ON k.id=h.principal_id`

func scanEmailHuman(row interface{ Scan(...any) error }) (EmailHuman, error) {
	var h EmailHuman
	var permissionsJSON, projectsJSON, grantsJSON string
	if err := row.Scan(&h.ID, &h.Email, &h.DeploymentAdmin, &permissionsJSON, &projectsJSON, &h.CreatedAt, &h.RevokedAt, &grantsJSON); err != nil {
		return h, err
	}
	if json.Unmarshal([]byte(grantsJSON), &h.Grants) != nil || json.Unmarshal([]byte(permissionsJSON), &h.Permissions) != nil || json.Unmarshal([]byte(projectsJSON), &h.Projects) != nil {
		return h, errors.New("invalid email account grants")
	}
	return h, nil
}

// Canonicalize grants before storing or comparing them so idempotent retries
// and changes to individual project permissions have deterministic semantics.
func (s *Service) emailGrantSpec(ctx context.Context, explicit []HumanProjectGrant, projects, permissions []string) (EmailHuman, error) {
	var h EmailHuman
	if explicit != nil {
		if projects != nil || permissions != nil || len(explicit) == 0 || len(explicit) > 64 {
			return h, apiError(400, "supply grants or projects and permissions")
		}
	} else {
		if len(projects) == 0 || len(projects) > 64 {
			return h, apiError(400, "expected explicit project grants")
		}
		for _, id := range projects {
			explicit = append(explicit, HumanProjectGrant{ProjectID: id, Permissions: permissions})
		}
	}
	perProject := make(map[string][]string, len(explicit))
	for _, g := range explicit {
		if g.ProjectID == "*" || slices.Contains(g.Permissions, "instance:admin") {
			if len(explicit) != 1 || g.ProjectID != "*" || !slices.Equal(g.Permissions, []string{"instance:admin"}) {
				return h, apiError(400, "instance administration requires exactly one grant: instance:admin on *")
			}
		}
		if len(g.Permissions) == 0 || len(g.Permissions) > 32 {
			return h, apiError(400, "expected 1 to 32 permissions per grant")
		}
		perProject[g.ProjectID] = append(perProject[g.ProjectID], g.Permissions...)
	}
	for project, perms := range perProject {
		h.Projects = append(h.Projects, project)
		h.Permissions = append(h.Permissions, perms...)
	}
	h.Projects = sortedGrants(h.Projects)
	h.Permissions = sortedGrants(h.Permissions)
	if err := s.validateCredentialGrant(ctx, principal(ctx), "Email human", h.Permissions, h.Projects); err != nil {
		return h, err
	}
	for _, id := range h.Projects {
		h.Grants = append(h.Grants, HumanProjectGrant{ProjectID: id, Permissions: sortedGrants(perProject[id])})
	}
	return h, nil
}

func sortedGrants(grants []string) []string {
	g := slices.Clone(grants)
	slices.Sort(g)
	return slices.Compact(g)
}

func (s *Service) inviteHuman(ctx context.Context, input *InviteHumanInput) (*EmailHumanOutput, error) {
	email, err := auth.NormalizeEmail(input.Body.Email)
	if err != nil {
		return nil, apiError(400, "invalid email address")
	}
	h, err := s.emailGrantSpec(ctx, input.Body.Grants, input.Body.Projects, input.Body.Permissions)
	if err != nil {
		return nil, err
	}
	h.ID, h.Email, h.CreatedAt = newID(), email, now()
	tx, err := s.control.BeginTx(ctx, nil)
	if err != nil {
		return nil, apiError(503, "human account storage unavailable")
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		return nil, apiError(503, "human account storage unavailable")
	}
	existing, err := scanEmailHuman(tx.QueryRowContext(ctx, selectEmailHumans+` WHERE h.email=$1`, email))
	if err == nil {
		if existing.RevokedAt != nil || !slices.EqualFunc(existing.Grants, h.Grants, func(a, b HumanProjectGrant) bool {
			return a.ProjectID == b.ProjectID && slices.Equal(a.Permissions, b.Permissions)
		}) {
			return nil, apiError(409, "human already exists with different grants or is revoked")
		}
		h = existing
	} else if !errors.Is(err, sql.ErrNoRows) || insertEmailHuman(ctx, tx, h) != nil {
		return nil, apiError(503, "human account storage unavailable")
	}

	send := s.oauth.config.Email.SendInvitation
	attempt := ""
	if send != nil {
		var sent bool
		var retryAfter int64
		if err := tx.QueryRowContext(ctx, `SELECT notice_sent, notice_retry_after_ms FROM email_humans WHERE principal_id=$1`, h.ID).Scan(&sent, &retryAfter); err != nil {
			return nil, apiError(503, "invitation status unavailable")
		}
		if !sent {
			if s.clock().UnixMilli() < retryAfter {
				return nil, apiError(429, "invitation delivery in progress or cooling down; retry the same request")
			}
			attempt = newID()
			if _, err := tx.ExecContext(ctx, `UPDATE email_humans SET notice_attempt=$1, notice_retry_after_ms=$2 WHERE principal_id=$3`, attempt, s.clock().Add(time.Minute).UnixMilli(), h.ID); err != nil {
				return nil, apiError(503, "invitation status unavailable")
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, apiError(503, "human account storage unavailable")
	}
	if attempt != "" {
		sendCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		sendErr := send(sendCtx, h.Email, slices.Contains(h.Permissions, "instance:admin"))
		cancel()
		// If the request disconnected during delivery, still save the result.
		writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		result, err := s.control.ExecContext(writeCtx, `UPDATE email_humans SET notice_sent=$1, notice_attempt='',notice_retry_after_ms=$2 WHERE principal_id=$3 AND notice_attempt=$4`, sendErr == nil, s.clock().Add(5*time.Second).UnixMilli(), h.ID, attempt)
		if err != nil {
			return nil, apiError(503, "invitation status unavailable; retry the same request")
		}
		affected, err := result.RowsAffected()
		if err != nil || affected != 1 {
			return nil, apiError(503, "invitation status unavailable; retry the same request")
		}
		if sendErr != nil {
			return nil, apiError(503, "account authorized; retry this request to deliver the invitation notice")
		}
	}
	return &EmailHumanOutput{Body: h}, nil
}

func (s *Service) updateHuman(ctx context.Context, input *UpdateHumanInput) (*EmailHumanOutput, error) {
	spec, err := s.emailGrantSpec(ctx, input.Body.Grants, input.Body.Projects, input.Body.Permissions)
	if err != nil {
		return nil, err
	}
	permissionsJSON, _ := json.Marshal(spec.Permissions)
	projectsJSON, _ := json.Marshal(spec.Projects)
	grantsJSON, _ := json.Marshal(spec.Grants)
	tx, err := s.control.BeginTx(ctx, nil)
	if err != nil {
		return nil, apiError(503, "human account storage unavailable")
	}
	defer tx.Rollback()
	r, err := tx.ExecContext(ctx, `UPDATE access_keys SET permissions=$1,projects=$2 WHERE revoked_at IS NULL AND id=$3 AND id IN (SELECT principal_id FROM email_humans WHERE deployment_admin=0)`, string(permissionsJSON), string(projectsJSON), input.ID)
	if err != nil {
		return nil, apiError(503, "human account storage unavailable")
	}
	n, err := r.RowsAffected()
	if err != nil {
		return nil, apiError(503, "human account storage unavailable")
	}
	if n == 0 {
		return nil, apiError(404, "active invited human not found")
	}
	// The database invalidates connections on changed project grants even when both unions stay equal.
	if _, err := tx.ExecContext(ctx, `UPDATE email_humans SET project_grants=$1 WHERE principal_id=$2`, string(grantsJSON), input.ID); err != nil {
		return nil, apiError(503, "human account storage unavailable")
	}
	h, err := scanEmailHuman(tx.QueryRowContext(ctx, selectEmailHumans+` WHERE k.id=$1`, input.ID))
	if err != nil || tx.Commit() != nil {
		return nil, apiError(503, "human account storage unavailable")
	}
	return &EmailHumanOutput{Body: h}, nil
}

func (s *Service) listHumans(ctx context.Context, input *ListHumansInput) (*ListHumansOutput, error) {
	if input.Limit < 1 || input.Limit > 100 || input.Offset < 0 {
		return nil, apiError(400, "invalid pagination")
	}
	rows, err := s.control.QueryContext(ctx, selectEmailHumans+` ORDER BY h.email LIMIT $1 OFFSET $2`, input.Limit+1, input.Offset)
	if err != nil {
		return nil, apiError(503, "human account storage unavailable")
	}
	defer rows.Close()
	out := &ListHumansOutput{}
	out.Body.Humans = []EmailHuman{}
	for rows.Next() {
		h, err := scanEmailHuman(rows)
		if err != nil {
			return nil, apiError(503, "human account storage unavailable")
		}
		if len(out.Body.Humans) == input.Limit {
			next := input.Offset + input.Limit
			out.Body.NextOffset = &next
			break
		}
		out.Body.Humans = append(out.Body.Humans, h)
	}
	if rows.Err() != nil {
		return nil, apiError(503, "human account storage unavailable")
	}
	return out, nil
}

func (s *Service) revokeHuman(ctx context.Context, input *HumanIDInput) (*struct{}, error) {
	var id string
	err := s.control.QueryRowContext(ctx, `SELECT principal_id FROM email_humans WHERE principal_id=$1`, input.ID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, apiError(404, "human account not found")
	}
	if err != nil {
		return nil, apiError(503, "human account storage unavailable")
	}
	return s.managementRevokeKey(ctx, &AccessKeyDeleteInput{ID: id})
}
