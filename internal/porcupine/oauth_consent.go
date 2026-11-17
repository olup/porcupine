package porcupine

import (
	"crypto/subtle"
	"html/template"
	"net/http"
	"net/url"
	"slices"
	"strings"
)

var oidcConsentPage = template.Must(template.New("consent").Parse(`<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Authorize Porcupine access</title><style>body{max-width:38rem;margin:3rem auto;padding:0 1rem;font:17px/1.5 system-ui;color:#161616;background:white}fieldset{margin:1rem 0}label{display:block;padding:.4rem}button{font:inherit;padding:.5rem 1rem;margin:.5rem .5rem 0 0}code{overflow-wrap:anywhere}</style><main><h1>Authorize access</h1><p><strong>{{.ClientName}}</strong> requests access to Porcupine.</p><p>Client: <code>{{.ClientID}}</code></p><p>Return address: <code>{{.RedirectURI}}</code></p><p>Resource: <code>{{.Resource}}</code></p><p>Permissions: <code>{{.Scope}}</code></p>{{if .Admin}}<p><strong>Grant full instance administration, including management of all people, clients and projects.</strong></p>{{end}}<form method="post" action="/oauth/consent"><input type="hidden" name="csrf" value="{{.CSRF}}"><fieldset><legend>Select projects to share</legend>{{range .Projects}}<label><input type="checkbox" name="project" value="{{.}}"> {{if eq . "*"}}All projects (instance-wide administrator){{else}}{{.}}{{end}}</label>{{end}}</fieldset><button type="submit" name="decision" value="approve">Approve access</button><button type="submit" name="decision" value="deny">Deny</button></form></main></html>`))

func (s *Service) oauthOIDCConsent(w http.ResponseWriter, r *http.Request) {
	oidcPageHeaders(w)
	o := s.oauth.browser
	key, session := o.session(r)
	if session == nil || session.humanID == "" {
		http.Error(w, "Login session expired", 401)
		return
	}
	request, err := s.loadOAuthAuthorizationRequest(r.Context(), session.requestID)
	if err != nil {
		http.Error(w, "Authorization request expired", 400)
		return
	}
	human, err := s.loadOAuthHuman(r.Context(), s.control, session.humanID)
	if err != nil {
		http.Error(w, "Project access revoked", 403)
		return
	}
	client, err := loadOAuthClient(r.Context(), s.control, request.ClientID)
	if err != nil {
		http.Error(w, "Client access revoked", 403)
		return
	}
	permissions := strings.Fields(request.Scope)
	if human.emailHuman && onlyInteractiveReadScopes(permissions) {
		permissions = slices.DeleteFunc(permissions, func(permission string) bool { return !human.can(permission) })
	}
	if !s.interactiveScopesAllowed(human, client, request.Resource, permissions) {
		http.Error(w, "Requested permissions are not granted", 403)
		return
	}
	if tenantAdminConsent(permissions) {
		s.oauthTenantConsent(w, r, key, session, request, human)
		return
	}
	projectsOffered := oauthProjectIntersection(human.Projects, client.Projects)
	admin := slices.Contains(permissions, "instance:admin")
	if slices.Contains(projectsOffered, "*") && !admin {
		// A wildcard in both upstream grants still requires choosing concrete projects here.
		rows, queryErr := s.control.QueryContext(r.Context(), `SELECT id FROM projects ORDER BY id LIMIT 1000`)
		if queryErr != nil {
			http.Error(w, "Project access unavailable", 503)
			return
		}
		projectsOffered = []string{}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				http.Error(w, "Project access unavailable", 503)
				return
			}
			projectsOffered = append(projectsOffered, id)
		}
		queryErr = rows.Err()
		rows.Close()
		if queryErr != nil {
			http.Error(w, "Project access unavailable", 503)
			return
		}

	}
	projectsOffered = slices.DeleteFunc(projectsOffered, func(project string) bool {
		return !interactiveProjectsAllowed(human, client.actor, permissions, []string{project})
	})
	if len(projectsOffered) == 0 {
		http.Error(w, "No shared projects are granted", 403)
		return
	}
	if r.Method == http.MethodGet {
		w.Header().Set("Referrer-Policy", "same-origin")
		callback, _ := url.Parse(request.RedirectURI)
		w.Header().Set("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; form-action 'self' "+callback.Scheme+"://"+callback.Host+"; frame-ancestors 'none'; base-uri 'none'")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_ = oidcConsentPage.Execute(w, struct {
			ClientName, ClientID, RedirectURI, Resource, Scope, CSRF string
			Admin                                                    bool
			Projects                                                 []string
		}{request.ClientName, request.ClientID, request.RedirectURI, request.Resource, strings.Join(permissions, " "), session.csrf, admin, projectsOffered})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	if r.Header.Get("Origin") != s.oauth.issuer || r.URL.RawQuery != "" || r.ParseForm() != nil || len(r.PostForm["csrf"]) != 1 || subtle.ConstantTimeCompare([]byte(r.PostForm.Get("csrf")), []byte(session.csrf)) != 1 || len(r.PostForm["decision"]) != 1 {
		http.Error(w, "Invalid consent submission", 400)
		return
	}
	decision := r.PostForm.Get("decision")
	if decision != "deny" && decision != "approve" {
		http.Error(w, "Invalid consent decision", 400)
		return
	}
	projects := []string{}
	for _, project := range r.PostForm["project"] {
		if !slices.Contains(projectsOffered, project) || slices.Contains(projects, project) {
			http.Error(w, "Project is not granted", 403)
			return
		}
		projects = append(projects, project)
	}
	if decision == "approve" && len(projects) == 0 {
		http.Error(w, "Select a project", 400)
		return
	}
	if len(projects) > 64 {
		http.Error(w, "Select at most 64 projects", 400)
		return
	}
	if !o.consume(key) {
		http.Error(w, "Consent already used", 400)
		return
	}
	o.cookie(w, "", -1)
	if decision == "deny" {
		if _, err := s.control.ExecContext(r.Context(), `UPDATE oauth_authorization_requests SET consumed=1 WHERE request_hash=$1 AND consumed=0`, hash(request.ID)); err != nil {
			http.Error(w, "Authorization could not be completed", 503)
			return
		}
		target, _ := url.Parse(request.RedirectURI)
		query := target.Query()
		query.Set("error", "access_denied")
		query.Set("state", request.State)
		query.Set("iss", s.oauth.issuer)
		target.RawQuery = query.Encode()
		http.Redirect(w, r, target.String(), http.StatusSeeOther)
		return
	}
	approved := actor{ID: human.ID, Permissions: permissions, Projects: projects}
	location, err := s.issueOAuthAuthorizationCode(r.Context(), request, human.ID, session.proof, approved)
	if err != nil {
		http.Error(w, "Authorization could not be completed", 403)
		return
	}
	http.Redirect(w, r, location, http.StatusSeeOther)
}
