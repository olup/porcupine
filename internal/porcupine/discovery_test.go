package porcupine

import (
	"context"
	"strings"
	"testing"

	lakeapi "github.com/olup/porcupine/internal/lake"
)

func TestEventCatalogInputAndDisabledStorage(t *testing.T) {
	t.Parallel()
	s := service(t, 20)
	id, _ := project(t, s)
	for _, input := range []*lakeapi.EventCatalogInput{{ID: id, Limit: 101}, {ID: id, Limit: -1}, {ID: id, Offset: -1}, {ID: id, Offset: 10001}, {ID: id, Event: strings.Repeat("e", 501)}} {
		if _, err := s.eventCatalog(context.Background(), input); queryStatus(err) != 400 {
			t.Fatalf("invalid input accepted: %+v %v", input, err)
		}
	}
	if _, err := s.eventCatalog(context.Background(), &lakeapi.EventCatalogInput{ID: "missing"}); queryStatus(err) != 404 {
		t.Fatal("missing project accepted")
	}
	if _, err := s.eventCatalog(context.Background(), &lakeapi.EventCatalogInput{ID: id}); queryStatus(err) != 503 {
		t.Fatal("disabled analytics must return 503")
	}
}
func TestEventCatalogHTTPProjectAuthorization(t *testing.T) {
	t.Parallel()
	s := service(t, 20)
	a, _ := project(t, s)
	b, _ := project(t, s)
	_, writer := key(t, s, []string{"events:write"}, []string{a})
	_, reader := key(t, s, []string{"events:read"}, []string{a})
	request(t, s, "GET", "/api/v1/projects/"+a+"/catalog", reader, "", 503)
	request(t, s, "GET", "/api/v1/projects/"+b+"/catalog", reader, "", 403)
	request(t, s, "GET", "/api/v1/projects/"+a+"/catalog", writer, "", 403)
	request(t, s, "GET", "/api/v1/projects/"+a+"/catalog", "", "", 401)
}
