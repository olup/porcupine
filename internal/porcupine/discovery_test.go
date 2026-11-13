package porcupine

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
)

func catalogResponse(w http.ResponseWriter, rows any) {
	data, _ := json.Marshal(rows)
	var values []json.RawMessage
	json.Unmarshal(data, &values)
	fmt.Fprintf(w, `{"meta":[],"data":%s,"rows":%d}`, data, len(values))
}

func catalogEvent(name, uuid string) EventCatalogEvent {
	return EventCatalogEvent{Event: name, Count: "18446744073709551615", FirstReceivedAt: "2026-01-01T00:00:00Z", LastReceivedAt: "2026-11-07T21:00:00Z", LatestEventUUID: uuid, LatestEventTimestamp: "2025-01-01T00:00:00Z"}
}

func TestEventCatalogRestrictedProjectReadersAndBoundParameters(t *testing.T) {
	s := service(t, 20)
	a, _ := project(t, s)
	b, _ := project(t, s)
	filter := "purchase' UNION SELECT password FROM system.users --"
	var mu sync.Mutex
	var readers, databases []string
	queries := 0
	c := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		sql := string(body)
		if r.Header.Get("X-ClickHouse-User") == "admin_user" {
			if strings.HasPrefix(sql, "SELECT") {
				t.Error("catalog ran a SELECT as administrator")
			}
			return
		}
		if strings.Contains(sql, filter) || strings.Contains(sql, a) || strings.Contains(sql, b) {
			t.Error("input interpolated into trusted catalog SQL")
		}
		if r.URL.Query().Get("param_event") != filter {
			t.Error("event filter not passed as typed parameter")
		}
		user := r.Header.Get("X-ClickHouse-User")
		database := r.URL.Query().Get("database")
		if user != database+"_reader" || r.Header.Get("X-ClickHouse-Key") == "" {
			t.Error("query did not use restricted reader")
		}
		mu.Lock()
		readers = append(readers, user)
		databases = append(databases, database)
		queries++
		mu.Unlock()
		if strings.Contains(sql, "ARRAY JOIN") {
			if sql != catalogPropertiesSQL {
				t.Error("property query was modified")
			}
			catalogResponse(w, []EventCatalogProperty{{Key: "price", Types: []string{"Float64", "String"}, SampleOccurrences: "12", LatestEventUUID: "latest-price", LastReceivedAt: "2026-11-07T21:00:00Z"}})
			return
		}
		if r.URL.Query().Get("param_catalog_limit") != "3" || r.URL.Query().Get("param_catalog_offset") != "4" {
			t.Error("pagination not bound as typed parameters")
		}
		catalogResponse(w, []EventCatalogEvent{catalogEvent(filter, "latest")})
	})
	configureAnalyticsTest(s, c)
	if _, err := validateSQL(catalogPropertiesSQL); err == nil {
		t.Fatal("test must exercise trusted SQL beyond the client subset")
	}
	for _, projectID := range []string{a, b} {
		result, err := s.eventCatalog(context.Background(), &EventCatalogInput{ID: projectID, Event: filter, Limit: 2, Offset: 4})
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Body.Events) != 1 || result.Body.Events[0].Count != "18446744073709551615" || len(result.Body.Properties) != 1 || result.Body.Properties[0].LatestEventUUID != "latest-price" {
			t.Fatal("typed catalog response lost metadata or precision")
		}
		if !result.Body.Sampling.PropertiesRequested || result.Body.Sampling.HistoricalPropertiesComplete || result.Body.Sampling.SampleEventLimit != 1000 || result.Body.Sampling.KeysPerEventLimit != 200 || result.Body.Sampling.MaxPropertyNameBytes != 200 {
			t.Fatal("sampling limitations were not reported")
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if queries != 4 || readers[0] != readers[1] || readers[0] == readers[2] || databases[0] == databases[2] {
		t.Fatal("catalog readers were shared across projects")
	}
}

func TestEventCatalogPaginationPropertyCapsAndNoValues(t *testing.T) {
	s := service(t, 20)
	id, _ := project(t, s)
	c := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-ClickHouse-User") == "admin_user" {
			return
		}
		body, _ := io.ReadAll(r.Body)
		if strings.Contains(string(body), "ARRAY JOIN") {
			rows := make([]map[string]any, 101)
			for i := range rows {
				rows[i] = map[string]any{"key": fmt.Sprintf("key_%03d", i), "types": []string{"String"}, "sample_occurrences": "1", "latest_event_uuid": "pointer", "last_received_at": "2026-11-07T21:00:00Z", "sample_value": "private actual value", "properties": map[string]string{"private": "private actual value"}}
			}
			catalogResponse(w, rows)
			return
		}
		catalogResponse(w, []EventCatalogEvent{catalogEvent("a", "a"), catalogEvent("b", "b"), catalogEvent("c", "c")})
	})
	configureAnalyticsTest(s, c)
	result, err := s.eventCatalog(context.Background(), &EventCatalogInput{ID: id, Event: "a", Limit: 2, Offset: 8, PropertyLimit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Body.Events) != 2 || result.Body.NextOffset == nil || *result.Body.NextOffset != 10 || len(result.Body.Properties) != 100 || !result.Body.Sampling.PropertiesTruncated {
		t.Fatal("catalog did not enforce response caps")
	}
	encoded, _ := json.Marshal(result.Body)
	if strings.Contains(string(encoded), "private actual value") || strings.Contains(string(encoded), "sample_value") {
		t.Fatal("property values leaked")
	}
	result, err = s.eventCatalog(context.Background(), &EventCatalogInput{ID: id, Limit: 2, Offset: 10000})
	if err != nil {
		t.Fatal(err)
	}
	if result.Body.NextOffset != nil || !result.Body.Sampling.PaginationTruncated || len(result.Body.Properties) != 0 || result.Body.Sampling.PropertiesRequested {
		t.Fatal("pagination ceiling or unrequested property discovery was not reported")
	}
}

func TestEventCatalogValidationStorageQuotaAndNoPartialResults(t *testing.T) {
	s := service(t, 20)
	id, _ := project(t, s)
	for _, input := range []*EventCatalogInput{{ID: id, Limit: 101}, {ID: id, Limit: -1}, {ID: id, Offset: -1}, {ID: id, Offset: 10001}, {ID: id, Event: strings.Repeat("e", 501)}} {
		if _, err := s.eventCatalog(context.Background(), input); queryStatus(err) != 400 {
			t.Fatalf("invalid input accepted: %+v %v", input, err)
		}
	}
	if _, err := s.eventCatalog(context.Background(), &EventCatalogInput{ID: "missing"}); queryStatus(err) != 404 {
		t.Fatal("missing project accepted")
	}
	if _, err := s.eventCatalog(context.Background(), &EventCatalogInput{ID: id}); queryStatus(err) != 503 {
		t.Fatal("disabled ClickHouse did not return 503")
	}
	c := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-ClickHouse-User") == "admin_user" {
			return
		}
		body, _ := io.ReadAll(r.Body)
		if strings.Contains(string(body), "ARRAY JOIN") {
			w.Header().Set("X-ClickHouse-Exception-Code", "241")
			w.WriteHeader(500)
			io.WriteString(w, "private SQL and credentials")
			return
		}
		catalogResponse(w, []EventCatalogEvent{catalogEvent("purchase", "pointer")})
	})
	configureAnalyticsTest(s, c)
	result, err := s.eventCatalog(context.Background(), &EventCatalogInput{ID: id, Event: "purchase"})
	if result != nil || queryStatus(err) != 429 || strings.Contains(err.Error(), "private") {
		t.Fatal("quota failure returned partial results or leaked server error")
	}
	s.querySlots <- struct{}{}
	s.querySlots <- struct{}{}
	if _, err = s.eventCatalog(context.Background(), &EventCatalogInput{ID: id}); queryStatus(err) != 429 {
		t.Fatal("catalog bypassed shared query concurrency limit")
	}
	<-s.querySlots
	<-s.querySlots
}

func TestEventCatalogRejectsMalformedOrExcessStorageRows(t *testing.T) {
	for _, response := range []string{
		`{"meta":[],"data":[],"rows":1}`,
		`{"meta":[],"data":[{"event":"a","count":5}],"rows":1}`,
		`{"meta":[],"data":[{"event":"a","count":"invalid"}],"rows":1}`,
		`{"data":[],"rows":0}`,
	} {
		t.Run(response, func(t *testing.T) {
			s := service(t, 20)
			id, _ := project(t, s)
			c := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("X-ClickHouse-User") != "admin_user" {
					io.WriteString(w, response)
				}
			})
			configureAnalyticsTest(s, c)
			if result, err := s.eventCatalog(context.Background(), &EventCatalogInput{ID: id}); result != nil || queryStatus(err) != 503 {
				t.Fatal("invalid storage envelope accepted")
			}
		})
	}
	s := service(t, 20)
	id, _ := project(t, s)
	c := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-ClickHouse-User") != "admin_user" {
			catalogResponse(w, []EventCatalogEvent{catalogEvent("a", "a"), catalogEvent("b", "b"), catalogEvent("c", "c")})
		}
	})
	configureAnalyticsTest(s, c)
	if result, err := s.eventCatalog(context.Background(), &EventCatalogInput{ID: id, Limit: 1}); result != nil || queryStatus(err) != 503 {
		t.Fatal("storage row count exceeded expected limit")
	}
}

func TestEventCatalogHTTPProjectAuthorization(t *testing.T) {
	s := service(t, 20)
	a, ingest := project(t, s)
	b, _ := project(t, s)
	_, reader := key(t, s, []string{"events:read"}, []string{a})
	c := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-ClickHouse-User") != "admin_user" {
			catalogResponse(w, []EventCatalogEvent{})
		}
	})
	configureAnalyticsTest(s, c)
	request(t, s, "GET", "/api/v1/projects/"+a+"/catalog", reader, "", 200)
	request(t, s, "GET", "/api/v1/projects/"+b+"/catalog", reader, "", 403)
	request(t, s, "GET", "/api/v1/projects/"+a+"/catalog", ingest, "", 403)
}
