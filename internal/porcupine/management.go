package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
)

type Project struct {
	IngestionMode string `json:"ingestion_mode" enum:"mixed,server_only" doc:"mixed permits public ingestion keys; server_only requires an API-resource access token or service key in the Authorization header."`
	ID            string `json:"id" doc:"Project identifier"`
	Name          string `json:"name" doc:"Project name"`
	CreatedAt     string `json:"created_at" doc:"Creation time in UTC"`
}

type ProjectList struct {
	Projects []Project `json:"projects"`
}
type ProjectListOutput struct{ Body ProjectList }
type ProjectCreateInput struct {
	Body struct {
		Name          string `json:"name" minLength:"1" maxLength:"200" doc:"Project name"`
		IngestionMode string `json:"ingestion_mode,omitempty" enum:"mixed,server_only" default:"mixed" doc:"server_only requires configured OAuth, creates no public ingestion key, and requires an API-resource access token or service key on every capture route."`
	}
}
type ProjectCreated struct {
	Project     Project `json:"project"`
	IngestToken string  `json:"ingest_token,omitempty" doc:"One-time public single-project ingestion key for mixed projects only. Absent for server_only projects."`
}
type ProjectCreateOutput struct{ Body ProjectCreated }

type ProjectIDInput struct {
	ID string `path:"id" doc:"Project identifier"`
}
type ProjectRenameInput struct {
	ID   string `path:"id" doc:"Project identifier"`
	Body struct {
		Name string `json:"name" minLength:"1" maxLength:"200" doc:"New display name (up to 200 UTF-8 bytes)."`
	}
}
type ProjectRenameOutput struct {
	Body struct {
		Project Project `json:"project"`
	}
}

// AccessKey contains public credential metadata. Secrets and hashes have no fields here.
type AccessKey struct {
	ServiceClientID string   `json:"service_client_id,omitempty" doc:"Owning machine client for a static service key."`
	Audience        string   `json:"audience,omitempty" doc:"API or MCP resource for a static service key."`
	ID              string   `json:"id"`
	Name            string   `json:"name"`
	SourceID        string   `json:"source_id,omitempty" doc:"Credential-bound logical producer for ingestion; blank uses protocol defaults."`
	Permissions     []string `json:"permissions"`
	Projects        []string `json:"projects"`
	CreatedAt       string   `json:"created_at"`
	RevokedAt       *string  `json:"revoked_at" nullable:"true"`
}
type AccessKeyList struct {
	AccessKeys []AccessKey `json:"access_keys"`
}
type AccessKeyListOutput struct{ Body AccessKeyList }
type AccessKeyCreateInput struct {
	Body struct {
		Name        string   `json:"name" minLength:"1" maxLength:"200"`
		SourceID    string   `json:"source_id,omitempty" maxLength:"128" pattern:"^([A-Za-z0-9][A-Za-z0-9._:-]{0,127})?$" doc:"Optional logical producer, requiring events:write and exactly one project. Immutable after creation; posthog is reserved for unbound ingestion."`
		Permissions []string `json:"permissions" minItems:"1" doc:"Permissions: instance:admin, management:read, management:write, events:read, events:write, events:edit, events:delete, identity:write, pipelines:read, pipelines:write"`
		Projects    []string `json:"projects" minItems:"1" doc:"Project IDs, or * for instance-wide scope"`
	}
}
type AccessKeyCreated struct {
	ServiceClientID string   `json:"service_client_id,omitempty" doc:"Owning machine client for a static service key."`
	Audience        string   `json:"audience,omitempty" doc:"API or MCP resource for a static service key."`
	ID              string   `json:"id"`
	Name            string   `json:"name"`
	SourceID        string   `json:"source_id,omitempty" doc:"Credential-bound logical producer for ingestion; blank uses protocol defaults."`
	Permissions     []string `json:"permissions"`
	Projects        []string `json:"projects"`
	Token           string   `json:"token" doc:"One-time secret. Store securely; it cannot be retrieved later."`
}
type AccessKeyCreateOutput struct{ Body AccessKeyCreated }
type AccessKeyDeleteInput struct {
	ID string `path:"id" doc:"Access key identifier"`
}
type EventListInput struct {
	ID    string `path:"id" doc:"Project identifier"`
	Limit int    `query:"limit" default:"100" minimum:"1" maximum:"1000"`
}
type EventList struct {
	Events []Event `json:"events"`
}
type EventListOutput struct{ Body EventList }
type managementError struct {
	Message string `json:"error"`
	status  int
}

func (e *managementError) Error() string  { return e.Message }
func (e *managementError) GetStatus() int { return e.status }
func apiError(status int, message string) error {
	return &managementError{Message: message, status: status}
}

type actorContextKey struct{}

func principal(ctx context.Context) actor { return ctx.Value(actorContextKey{}).(actor) }

func (s *Service) registerManagement(m *http.ServeMux) huma.API {
	config := huma.DefaultConfig("Porcupine management API", "1.0.0")
	config.OpenAPIPath = "/openapi"
	config.DocsPath = ""
	config.SchemasPath = ""
	config.CreateHooks = nil
	config.Components.SecuritySchemes = map[string]*huma.SecurityScheme{
		"bearerAuth": {Type: "http", Scheme: "bearer", Description: "Porcupine access key"},
	}
	if s.oauth != nil {
		scopes := map[string]string{}
		for _, permission := range validPermissions {
			scopes[permission] = permission
		}
		tokenURL := s.oauth.issuer + "/oauth/token"
		flows := &huma.OAuthFlows{ClientCredentials: &huma.OAuthFlow{TokenURL: tokenURL, Scopes: scopes}}
		if s.oauth.config.Email != nil {
			flows.AuthorizationCode = &huma.OAuthFlow{AuthorizationURL: s.oauth.issuer + "/oauth/authorize", TokenURL: tokenURL, RefreshURL: tokenURL, Scopes: scopes}
		}
		config.Components.SecuritySchemes = map[string]*huma.SecurityScheme{
			"serviceKey": {Type: "http", Scheme: "bearer", Description: "Static service key for the API resource."},
			"oauth2":     {Type: "oauth2", Description: "Short-lived access token issued for the Porcupine API resource. Interactive clients request this exact resource with S256 PKCE.", Flows: flows},
		}
	}
	// Keep the service's existing error envelope and avoid reflecting secret inputs.
	config.Formats = map[string]huma.Format{"application/json": {
		Marshal: func(w io.Writer, v any) error {
			if err, ok := v.(huma.StatusError); ok {
				return json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			}
			return json.NewEncoder(w).Encode(v)
		},
		Unmarshal: json.Unmarshal,
	}}
	config.Formats["application/problem+json"] = config.Formats["application/json"]
	api := humago.New(m, config)
	api.UseMiddleware(func(ctx huma.Context, next func(huma.Context)) {
		req, w := humago.Unwrap(ctx)
		w.Header().Set("Cache-Control", "no-store")
		access, _ := ctx.Operation().Metadata["access"].(operationAccess)
		if access.ProjectParameter == "" && ctx.Operation().OperationID != "delete-project" {
			s.catalogMu.RLock()
			defer s.catalogMu.RUnlock()
		}
		raw, err := bearerCredential(req)
		resource := ""
		if s.oauth != nil {
			resource = s.oauth.apiResource
			if internal, _ := ctx.Context().Value(mcpDispatchKey{}).(bool); internal {
				resource = s.oauth.mcpResource
			}
		}
		var a actor
		if err == nil {
			a, err = s.authenticateResource(ctx.Context(), raw, resource)
		}
		if err != nil {
			s.authChallenge(w, resource, req.Header.Get("Authorization") != "")
			problem(w, http.StatusUnauthorized, "invalid access credential")
			return
		}
		if !access.allows(a, ctx.Param(access.ProjectParameter)) {
			_, w := humago.Unwrap(ctx)
			problem(w, http.StatusForbidden, "required permission and project access missing")
			return
		}
		if access.ProjectParameter != "" && ctx.Operation().OperationID != "delete-project" {
			child, finish, active := s.activity.begin(ctx.Context(), ctx.Param(access.ProjectParameter))
			if !active {
				problem(w, http.StatusNotFound, "project unavailable")
				return
			}
			defer finish()
			ctx = huma.WithContext(ctx, child)
		}
		if encoding := strings.ToLower(strings.TrimSpace(ctx.Header("Content-Encoding"))); encoding != "" && encoding != "identity" {
			_, w := humago.Unwrap(ctx)
			problem(w, http.StatusUnsupportedMediaType, "unsupported body encoding")
			return
		}
		next(huma.WithValue(ctx, actorContextKey{}, a))
	})
	op := func(id, method, path, summary string, access operationAccess, status int) huma.Operation {
		responses := map[string]*huma.Response{}
		errorSchema := &huma.Schema{Type: "object", Properties: map[string]*huma.Schema{"error": {Type: "string"}}, Required: []string{"error"}}
		for _, code := range []int{400, 401, 403, 404, 409, 410, 413, 415, 422, 429, 500, 503} {
			responses[fmt.Sprint(code)] = &huma.Response{Description: http.StatusText(code), Content: map[string]*huma.MediaType{
				"application/json": {Schema: errorSchema}, "application/problem+json": {Schema: errorSchema},
			}}
		}
		security := []map[string][]string{{"bearerAuth": {}}}
		if s.oauth != nil {
			scopes := []string{access.Permission}
			for _, permission := range append(append([]string{}, access.InstancePermissions...), access.AdditionalPermissions...) {
				if !slices.Contains(scopes, permission) {
					scopes = append(scopes, permission)
				}
			}
			security = []map[string][]string{{"oauth2": scopes}, {"serviceKey": {}}}
		}
		return huma.Operation{OperationID: id, Method: method, Path: path, Summary: summary, DefaultStatus: status,
			MaxBodyBytes: maxBodyBytes, Security: security,
			Metadata: map[string]any{"access": access}, Responses: responses}
	}
	huma.Register(api, op("list-projects", "GET", "/api/v1/projects", "List projects accessible to this key", operationAccess{Permission: "management:read"}, 200), s.managementListProjects)
	huma.Register(api, op("create-project", "POST", "/api/v1/projects", "Create a project with mixed or OAuth-only server ingestion; mixed projects return a one-time public ingestion key", operationAccess{Permission: "management:write", InstancePermissions: []string{"management:write", "events:write"}}, 201), s.managementCreateProject)
	huma.Register(api, op("rename-project", "PATCH", "/api/v1/projects/{id}", "Set a project's display name, preserving its ID, data and credentials", operationAccess{Permission: "management:write", ProjectParameter: "id"}, 200), s.managementRenameProject)
	huma.Register(api, op("delete-project", "DELETE", "/api/v1/projects/{id}", "Permanently delete a project, its analytics data and its Porcupine-managed exports. Retry the same ID if cleanup is interrupted.", operationAccess{Permission: "instance:admin", ProjectParameter: "id"}, 204), s.managementDeleteProject)
	huma.Register(api, op("list-access-keys", "GET", "/api/v1/access-keys", "List in-scope key metadata without secrets", operationAccess{Permission: "management:read"}, 200), s.managementListKeys)
	huma.Register(api, op("create-access-key", "POST", "/api/v1/access-keys", "Create a scoped key and return its secret once; cannot exceed caller grants", operationAccess{Permission: "management:write"}, 201), s.managementCreateKey)
	huma.Register(api, op("revoke-access-key", "DELETE", "/api/v1/access-keys/{id}", "Revoke a key; the last active administrator is protected", operationAccess{Permission: "instance:admin"}, 204), s.managementRevokeKey)
	huma.Register(api, op("list-project-events", "GET", "/api/v1/projects/{id}/events", "Inspect recent queued events awaiting delivery; use query-events for analytical history", operationAccess{Permission: "events:read", ProjectParameter: "id"}, 200), s.managementListEvents)
	huma.Register(api, op("get-event-schema", "GET", "/api/v1/projects/{id}/schema", "Discover the project SQL schema, query limits and delivery status", operationAccess{Permission: "events:read", ProjectParameter: "id"}, 200), s.analyticsSchema)
	queryOperation := op("query-events", "POST", "/api/v1/projects/{id}/query", "Execute agent-written read-only ClickHouse SQL against this project's events", operationAccess{Permission: "events:read", ProjectParameter: "id"}, 200)
	queryOperation.Description = `Agents write the SQL. First discover the table, allowed functions and limits with get-event-schema, then inspect observed names and properties with get-event-catalog.

Example counting conversations by day and a custom client property:
SELECT toDate(event_time) AS day, JSONExtractString(properties,'client_id') AS client, count() AS conversations FROM events WHERE event={event:String} GROUP BY day,client ORDER BY day,client
Parameters: {"event":"conversation_started"}.

Example inspecting the latest event pointer returned by the event catalog:
SELECT uuid,event,properties,timestamp FROM events WHERE uuid={uuid:String} LIMIT 1
Parameters: {"uuid":"UUID_FROM_CATALOG"}.

Queries see only this project's delivered logical events. event_time uses UTC milliseconds. Counts and other 64-bit integers use decimal strings in JSON. Property discovery is sampled and does not define metrics. Results are eventually consistent with ingestion; inspect delivery.pending. No writes, external sources, qualified tables, system metadata, FORMAT or SETTINGS.`
	huma.Register(api, queryOperation, s.analyticsQuery)
	huma.Register(api, op("get-event-catalog", "GET", "/api/v1/projects/{id}/catalog", "Discover observed event names, property types and latest-event UUID pointers; property discovery is sampled", operationAccess{Permission: "events:read", ProjectParameter: "id"}, 200), s.eventCatalog)
	huma.Register(api, op("get-event", "GET", "/api/v1/projects/{id}/events/{uuid}", "Read the current accepted event and revision, including a pending correction", operationAccess{Permission: "events:read", ProjectParameter: "id"}, 200), s.getEvent)
	huma.Register(api, op("edit-event", "PATCH", "/api/v1/projects/{id}/events/{uuid}", "Queue an audited full event correction with expected_revision and idempotency_key", operationAccess{Permission: "events:edit", ProjectParameter: "id"}, 202), s.editEvent)
	huma.Register(api, op("delete-event", "DELETE", "/api/v1/projects/{id}/events/{uuid}", "Queue a deletion tombstone; analytical visibility changes after projection and physical cleanup is asynchronous", operationAccess{Permission: "events:delete", ProjectParameter: "id"}, 202), s.deleteEvent)
	huma.Register(api, op("get-event-mutation", "GET", "/api/v1/projects/{id}/event-mutations/{mutation_id}", "Read whether an event correction or deletion is pending or applied", operationAccess{Permission: "events:read", ProjectParameter: "id"}, 200), s.getEventMutation)
	s.registerQuarantine(api, op)
	s.registerImportUploads(api, op)
	s.registerExports(api, op)
	s.registerOAuthClients(api, op)
	s.registerServiceKeys(api, op)
	s.registerEmailAccounts(api, op)
	s.registerPipelines(api, op)
	return api
}

func (s *Service) managementListProjects(ctx context.Context, _ *struct{}) (*ProjectListOutput, error) {
	a := principal(ctx)
	rows, err := s.db.QueryContext(ctx, `SELECT id,name,created_at,ingestion_mode FROM projects ORDER BY created_at,id`)
	if err != nil {
		return nil, apiError(500, "storage error")
	}
	defer rows.Close()
	items := []Project{}
	for rows.Next() {
		var p Project
		if err := rows.Scan(&p.ID, &p.Name, &p.CreatedAt, &p.IngestionMode); err != nil {
			return nil, apiError(500, "storage error")
		}
		if authorize(a, "management:read", p.ID) {
			items = append(items, p)
		}
	}
	if rows.Err() != nil {
		return nil, apiError(500, "storage error")
	}
	return &ProjectListOutput{Body: ProjectList{Projects: items}}, nil
}

func projectName(raw string) (string, error) {
	name := strings.TrimSpace(raw)
	if name == "" || len(name) > 200 {
		return "", apiError(400, "name must contain 1 to 200 UTF-8 bytes")
	}
	return name, nil
}

func (s *Service) managementRenameProject(ctx context.Context, input *ProjectRenameInput) (*ProjectRenameOutput, error) {
	name, err := projectName(input.Body.Name)
	if err != nil {
		return nil, err
	}
	out := &ProjectRenameOutput{}
	p := &out.Body.Project
	err = s.db.QueryRowContext(ctx, `UPDATE projects SET name=? WHERE id=? RETURNING id,name,created_at,ingestion_mode`, name, input.ID).Scan(&p.ID, &p.Name, &p.CreatedAt, &p.IngestionMode)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, apiError(404, "project not found")
	}
	if err != nil {
		return nil, apiError(500, "storage error")
	}
	return out, nil
}

func (s *Service) managementDeleteProject(ctx context.Context, input *ProjectIDInput) (*struct{}, error) {
	return nil, s.deleteProject(ctx, input.ID)
}

func (s *Service) managementCreateProject(ctx context.Context, input *ProjectCreateInput) (*ProjectCreateOutput, error) {
	name, err := projectName(input.Body.Name)
	if err != nil {
		return nil, err
	}
	mode := input.Body.IngestionMode
	if mode == "" {
		mode = ingestionMixed
	}
	if mode != ingestionMixed && mode != ingestionServerOnly {
		return nil, apiError(400, "ingestion_mode must be mixed or server_only")
	}
	if mode == ingestionServerOnly && s.oauth == nil {
		return nil, apiError(503, "server_only ingestion requires configured OAuth")
	}
	p := Project{ID: newID(), Name: name, CreatedAt: now(), IngestionMode: mode}
	raw := ""
	if mode == ingestionMixed {
		raw = token()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, apiError(500, "storage error")
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO projects(id,name,created_at,ingestion_mode) VALUES(?,?,?,?)`, p.ID, p.Name, p.CreatedAt, p.IngestionMode)
	if err == nil && mode == ingestionMixed {
		projects, _ := json.Marshal([]string{p.ID})
		_, err = tx.ExecContext(ctx, `INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at) VALUES(?,?,?,?,?,?)`, newID(), p.Name+" ingestion", hash(raw), `["events:write"]`, string(projects), p.CreatedAt)
	}
	if err != nil || tx.Commit() != nil {
		return nil, apiError(500, "storage error")
	}
	return &ProjectCreateOutput{Body: ProjectCreated{Project: p, IngestToken: raw}}, nil
}

func (s *Service) managementListKeys(ctx context.Context, _ *struct{}) (*AccessKeyListOutput, error) {
	a := principal(ctx)
	rows, err := s.db.QueryContext(ctx, `SELECT k.id,k.name,k.permissions,k.projects,k.created_at,k.revoked_at,k.source_id,
		coalesce(b.client_id,''),coalesce(b.audience,'') FROM access_keys k LEFT JOIN service_api_keys b ON b.principal_id=k.id ORDER BY k.created_at,k.id`)
	if err != nil {
		return nil, apiError(500, "storage error")
	}
	defer rows.Close()
	items := []AccessKey{}
	for rows.Next() {
		var key AccessKey
		var permissions, projects string
		if err = rows.Scan(&key.ID, &key.Name, &permissions, &projects, &key.CreatedAt, &key.RevokedAt, &key.SourceID, &key.ServiceClientID, &key.Audience); err != nil {
			return nil, apiError(500, "storage error")
		}
		if json.Unmarshal([]byte(permissions), &key.Permissions) != nil || json.Unmarshal([]byte(projects), &key.Projects) != nil {
			return nil, apiError(500, "storage error")
		}
		if a.containsProjects(key.Projects) {
			items = append(items, key)
		}
	}
	if rows.Err() != nil {
		return nil, apiError(500, "storage error")
	}
	return &AccessKeyListOutput{Body: AccessKeyList{AccessKeys: items}}, nil
}

func (s *Service) validateCredentialGrant(ctx context.Context, a actor, name string, permissions, projects []string) error {
	if strings.TrimSpace(name) == "" || len(name) > 200 || len(permissions) == 0 || len(projects) == 0 {
		return apiError(400, "name, permissions and projects are required")
	}
	for _, permission := range permissions {
		if !slices.Contains(validPermissions, permission) {
			return apiError(400, "unknown permission")
		}
		if !authorize(a, permission, "") {
			return apiError(403, "cannot grant permissions you do not hold")
		}
	}
	if !a.containsProjects(projects) {
		return apiError(403, "cannot grant projects outside your scope")
	}
	for _, project := range projects {
		if project != "*" {
			var n int
			if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM projects WHERE id=?`, project).Scan(&n); err != nil {
				return apiError(500, "storage error")
			}
			if n == 0 {
				return apiError(400, "unknown project")
			}
		}
	}
	if slices.Contains(permissions, "instance:admin") && !slices.Contains(projects, "*") {
		return apiError(400, "instance:admin requires wildcard project scope")
	}
	return nil
}

func (s *Service) managementCreateKey(ctx context.Context, input *AccessKeyCreateInput) (*AccessKeyCreateOutput, error) {
	a, body := principal(ctx), input.Body
	if !a.canProvisionCredentials() {
		return nil, apiError(403, "instance administrator required to provision independent credentials")
	}
	if err := s.validateCredentialGrant(ctx, a, body.Name, body.Permissions, body.Projects); err != nil {
		return nil, err
	}
	sourceID, err := delegatedSourceID(a, body.SourceID, body.Permissions, body.Projects)
	if err != nil {
		return nil, err
	}
	body.SourceID = sourceID
	if s.oauth != nil && !s.oauth.config.AllowLegacyKeys && (len(body.Permissions) != 1 || body.Permissions[0] != "events:write" || len(body.Projects) != 1 || body.Projects[0] == "*") {
		return nil, apiError(400, "use create-oauth-client for service access; keys are public single-project ingestion credentials")
	}
	id, raw := newID(), token()
	permissions, _ := json.Marshal(body.Permissions)
	projects, _ := json.Marshal(body.Projects)
	_, err = s.db.ExecContext(ctx, `INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at,source_id) VALUES(?,?,?,?,?,?,?)`, id, body.Name, hash(raw), string(permissions), string(projects), now(), body.SourceID)
	if err != nil {
		return nil, apiError(500, "storage error")
	}
	return &AccessKeyCreateOutput{Body: AccessKeyCreated{ID: id, Name: body.Name, SourceID: body.SourceID, Permissions: body.Permissions, Projects: body.Projects, Token: raw}}, nil
}

func (s *Service) managementRevokeKey(ctx context.Context, input *AccessKeyDeleteInput) (*struct{}, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, apiError(500, "storage error")
	}
	defer tx.Rollback()
	// Serialize administrator counts and revocation, including concurrent requests.
	if _, err = tx.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		return nil, apiError(500, "storage error")
	}
	var permissions string
	err = tx.QueryRowContext(ctx, `SELECT permissions FROM access_keys WHERE id=? AND revoked_at IS NULL`, input.ID).Scan(&permissions)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, apiError(404, "active key not found")
	}
	if err != nil {
		return nil, apiError(500, "storage error")
	}
	var granted []string
	if json.Unmarshal([]byte(permissions), &granted) != nil {
		return nil, apiError(500, "storage error")
	}
	if slices.Contains(granted, "instance:admin") {
		// Human grants, interactive-only client registrations, and service keys are
		// dependent credentials. Keep an independent recovery administrator.
		var replacement bool
		static := s.oauth == nil || s.oauth.config.AllowLegacyKeys
		err = tx.QueryRowContext(ctx, `SELECT EXISTS(
 SELECT 1 FROM access_keys k WHERE k.id<>? AND k.revoked_at IS NULL
 AND EXISTS(SELECT 1 FROM json_each(k.permissions) WHERE value='instance:admin')
 AND (k.id=(SELECT value FROM metadata WHERE key='oauth_bootstrap_id')
 OR (? AND EXISTS(SELECT 1 FROM oauth_clients c, json_each(c.grant_types) g
 WHERE c.client_id=k.id AND g.value='client_credentials'))
 OR (? AND k.id NOT IN (
 SELECT client_id FROM oauth_clients UNION ALL SELECT principal_id FROM email_humans
 UNION ALL SELECT principal_id FROM oidc_humans
 UNION ALL SELECT principal_id FROM service_api_keys UNION ALL SELECT principal_id FROM oauth_interactive_grants))))`,
			input.ID, s.oauth != nil, static).Scan(&replacement)
		if err != nil {
			return nil, apiError(500, "storage error")
		}
		if !replacement {
			return nil, apiError(409, "cannot revoke the last independent administrator")
		}
	}
	if _, err = tx.ExecContext(ctx, `UPDATE access_keys SET revoked_at=? WHERE id=?`, now(), input.ID); err != nil {
		return nil, apiError(500, "storage error")
	}
	if err = tx.Commit(); err != nil {
		return nil, apiError(500, "storage error")
	}
	return nil, nil
}

func (s *Service) managementListEvents(ctx context.Context, input *EventListInput) (*EventListOutput, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,project_id,uuid,event,distinct_id,properties,timestamp,received_at,delivered,ingestion_metadata,is_imported FROM events WHERE project_id=? AND identity_snapshot='' ORDER BY id DESC LIMIT ?`, input.ID, input.Limit)
	if err != nil {
		return nil, apiError(500, "storage error")
	}
	defer rows.Close()
	events := []Event{}
	for rows.Next() {
		var e Event
		var props, ingestion string
		if err = rows.Scan(&e.ID, &e.ProjectID, &e.UUID, &e.Event, &e.DistinctID, &props, &e.Timestamp, &e.ReceivedAt, &e.Delivered, &ingestion, &e.Imported); err != nil {
			return nil, apiError(500, "storage error")
		}
		if err := decodeIngestionMetadata(&e, ingestion); err != nil {
			return nil, apiError(503, "event storage unavailable")
		}
		e.Properties = json.RawMessage(props)
		events = append(events, e)
	}
	if rows.Err() != nil {
		return nil, apiError(500, "storage error")
	}
	return &EventListOutput{Body: EventList{Events: events}}, nil
}
