package porcupine

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"html/template"
	"net/http"
	"strings"
	"time"

	"github.com/danielgtaylor/huma/v2"
)

type InvitationCreateInput struct {
	ID string `path:"id"`
}
type InvitationCreated struct {
	ID        string    `json:"id"`
	Code      string    `json:"code" doc:"Delivered once. Share privately with the invited person."`
	ProjectID string    `json:"project_id"`
	ExpiresAt time.Time `json:"expires_at"`
}
type InvitationCreateOutput struct {
	CacheControl string `header:"Cache-Control"`
	Body         InvitationCreated
}
type InvitationRevokeInput struct {
	ID string `path:"id"`
}

func (s *Service) registerLocalInvitations(api huma.API, op func(string, string, string, string, operationAccess, int) huma.Operation) {
	if s.oauth == nil || !s.oauth.config.LocalInvitations {
		return
	}
	huma.Register(api, op("create-local-invitation", "POST", "/api/v1/projects/{id}/invitations", "Invite one person to connect a read-only MCP client to this project within 24 hours", operationAccess{Permission: "instance:admin", ProjectParameter: "id"}, 201), s.createLocalInvitation)
	huma.Register(api, op("revoke-local-invitation", "DELETE", "/api/v1/invitations/{id}", "Invalidate the invitation and any MCP access granted through it", operationAccess{Permission: "instance:admin"}, 204), s.revokeLocalInvitation)
}

func (s *Service) createLocalInvitation(ctx context.Context, in *InvitationCreateInput) (*InvitationCreateOutput, error) {
	code, err := oauthRandom("pc_inv_")
	if err != nil {
		return nil, apiError(503, "invitation unavailable")
	}
	id := newID()
	expires := s.clock().Add(24 * time.Hour).UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, apiError(503, "invitation storage unavailable")
	}
	defer tx.Rollback()
	projects, _ := json.Marshal([]string{in.ID})
	permissions, _ := json.Marshal(interactiveOAuthScopes)
	res, err := tx.ExecContext(ctx, `INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at)
 SELECT ?,'Local invitation',?,?,?,? FROM projects WHERE id=?`, id, hash("unissued-"+oidcRandom()), string(permissions), string(projects), now(), in.ID)
	if err != nil {
		return nil, apiError(503, "invitation storage unavailable")
	}
	count, err := res.RowsAffected()
	if err != nil {
		return nil, apiError(503, "invitation storage unavailable")
	}
	if count == 0 {
		return nil, apiError(404, "project not found")
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO local_invitations(principal_id,code_hash,expires_at_ms) VALUES(?,?,?)`, id, hash(code), expires.UnixMilli())
	if err != nil || tx.Commit() != nil {
		return nil, apiError(503, "invitation storage unavailable")
	}
	return &InvitationCreateOutput{CacheControl: "no-store", Body: InvitationCreated{ID: id, ProjectID: in.ID, Code: code, ExpiresAt: expires}}, nil
}

func (s *Service) revokeLocalInvitation(ctx context.Context, in *InvitationRevokeInput) (*struct{}, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE access_keys SET revoked_at=coalesce(revoked_at,?) WHERE id=? AND id IN (SELECT principal_id FROM local_invitations)`, now(), in.ID)
	if err != nil {
		return nil, apiError(503, "invitation storage unavailable")
	}
	n, err := res.RowsAffected()
	if err != nil {
		return nil, apiError(503, "invitation storage unavailable")
	}
	if n == 0 {
		return nil, apiError(404, "invitation not found")
	}
	return nil, nil
}

func loadInvitedHuman(ctx context.Context, query oauthQueryer, id string) (actor, error) {
	var permissions, projects string
	a := actor{ID: id}
	err := query.QueryRowContext(ctx, `SELECT k.permissions,k.projects FROM local_invitations i JOIN access_keys k ON i.principal_id=k.id WHERE k.id=? AND k.revoked_at IS NULL`, id).Scan(&permissions, &projects)
	if err != nil {
		return actor{}, err
	}
	if json.Unmarshal([]byte(permissions), &a.Permissions) != nil || json.Unmarshal([]byte(projects), &a.Projects) != nil {
		return actor{}, errors.New("invalid invitation grant")
	}
	return a, nil
}

var invitationPage = template.Must(template.New("invite").Parse(`<!doctype html><html lang="en"><meta charset="utf-8"><title>Porcupine invitation</title><main><h1>Connect {{.ClientName}} to Porcupine</h1><form action="/oauth/invitation" method="post"><input type="hidden" name="csrf" value="{{.CSRF}}"><label>Invitation code <input type="password" name="code" required autocomplete="off"></label> <button>Review access</button></form></main></html>`))

func (s *Service) startInvitationLogin(w http.ResponseWriter, request oauthAuthorizationRequest) {
	browser := s.oauth.browser
	session := &oidcBrowserSession{requestID: request.ID, csrf: oidcRandom(), expires: time.Now().Add(oidcSessionLifetime)}
	raw, ok := browser.save(session)
	if !ok {
		http.Error(w, "Login capacity reached", 503)
		return
	}
	browser.cookie(w, raw, int(oidcSessionLifetime.Seconds()))
	w.Header().Set("Referrer-Policy", "same-origin")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = invitationPage.Execute(w, struct{ ClientName, CSRF string }{request.ClientName, session.csrf})
}

func (s *Service) oauthInvitationLogin(w http.ResponseWriter, r *http.Request) {
	oidcPageHeaders(w)
	browser := s.oauth.browser
	key, session := browser.session(r)
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	if session == nil || session.humanID != "" || r.Header.Get("Origin") != s.oauth.issuer || r.URL.RawQuery != "" || r.ParseForm() != nil || len(r.PostForm["csrf"]) != 1 || subtle.ConstantTimeCompare([]byte(r.PostForm.Get("csrf")), []byte(session.csrf)) != 1 || len(r.PostForm["code"]) != 1 {
		http.Error(w, "Invalid login session", 400)
		return
	}
	if !browser.consume(key) {
		http.Error(w, "Login already used", 400)
		return
	}
	browser.cookie(w, "", -1)
	code := strings.TrimSpace(r.PostForm.Get("code"))
	var id string
	err := s.db.QueryRowContext(r.Context(), `SELECT i.principal_id FROM local_invitations i JOIN access_keys k ON k.id=i.principal_id WHERE code_hash=? AND used_at_ms IS NULL AND expires_at_ms>? AND k.revoked_at IS NULL`, hash(code), s.clock().UnixMilli()).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		http.Error(w, "Invalid or expired invitation", 403)
		return
	}
	if err != nil {
		http.Error(w, "Invitation storage unavailable", 503)
		return
	}
	session.humanID = id
	session.csrf = oidcRandom()
	raw, ok := browser.save(session)
	if !ok {
		http.Error(w, "Login capacity reached", 503)
		return
	}
	browser.cookie(w, raw, int(time.Until(session.expires).Seconds()))
	http.Redirect(w, r, "/oauth/consent", http.StatusSeeOther)
}
