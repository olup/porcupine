package porcupine

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func profileRequestPath(project, event string) string {
	return "/api/v1/projects/" + project + "/event-profile?event=" + url.QueryEscape(event)
}

func TestEventProfileHTTPAuthorizationAndDisabledStorage(t *testing.T) {
	s := service(t, 20)
	a, ingest := project(t, s)
	b, _ := project(t, s)
	_, reader := key(t, s, []string{"events:read"}, []string{a})
	_, manager := key(t, s, []string{"management:read"}, []string{a})
	path := profileRequestPath(a, "Message ' / ? é")
	request(t, s, http.MethodGet, path, reader, "", http.StatusServiceUnavailable)
	request(t, s, http.MethodGet, profileRequestPath(b, "Message"), reader, "", http.StatusForbidden)
	request(t, s, http.MethodGet, path, ingest, "", http.StatusForbidden)
	request(t, s, http.MethodGet, path, manager, "", http.StatusForbidden)
	request(t, s, http.MethodGet, path, "", "", http.StatusUnauthorized)
	request(t, s, http.MethodGet, profileRequestPath("missing-project", "Message"), adminToken, "", http.StatusNotFound)
}

func TestEventProfileHTTPRejectsUnboundedInputs(t *testing.T) {
	s := service(t, 20)
	a, _ := project(t, s)
	_, reader := key(t, s, []string{"events:read"}, []string{a})
	for _, suffix := range []string{
		"event=",
		"event=" + strings.Repeat("x", 501),
		"event=page&sample_limit=-1",
		"event=page&sample_limit=0",
		"event=page&sample_limit=1001",
		"event=page&example_limit=-1",
		"event=page&example_limit=6",
		"event=page&property_limit=101",
		"event=page&property_limit=0",
		"event=page&property_kind=anything",
		"event=page&property_prefix=" + strings.Repeat("x", 201),
		"event=page&include_values=perhaps",
	} {
		t.Run(suffix[:min(len(suffix), 70)], func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/api/v1/projects/"+a+"/event-profile?"+suffix, nil)
			r.Header.Set("Authorization", "Bearer "+reader)
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, r)
			if w.Code != http.StatusBadRequest && w.Code != http.StatusUnprocessableEntity {
				t.Fatalf("invalid profile input reached storage: status=%d body=%s", w.Code, w.Body.String())
			}
		})
	}
}
