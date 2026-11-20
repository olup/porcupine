package porcupine

import (
	"encoding/json"
	"net/http"
	"net/netip"
	"os"

	"github.com/olup/porcupine/internal/acceptance"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/oschwald/maxminddb-golang/v2"
)

func loadGeoIPCountry(path string) (func(netip.Addr) string, error) {
	if path == "" {
		return nil, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	db, err := maxminddb.OpenBytes(raw)
	if err != nil {
		return nil, err
	}
	return func(ip netip.Addr) string {
		var code string
		if db.Lookup(ip).DecodePath(&code, "country", "iso_code") != nil || len(code) != 2 ||
			code[0] < 'A' || code[0] > 'Z' || code[1] < 'A' || code[1] > 'Z' {
			return ""
		}
		return code
	}, nil
}

var sharedAddressSpace = netip.MustParsePrefix("100.64.0.0/10")

func (s *Service) enrichLiveGeoIP(r *http.Request, batch acceptance.Batch, inputs eventBatch, client netip.Addr) {
	origins := r.Header.Values("Origin")
	if len(origins) != 1 || !validIngestionOrigin(origins[0]) {
		return
	}
	var country json.RawMessage
	for i := range batch.Events {
		input := &batch.Events[i]
		props := inputs[i].Properties
		if input.Identity != nil || props["$geoip_disable"] == true || props["$is_server"] == true {
			continue
		}
		e := &input.Event
		var context map[string]json.RawMessage
		if len(e.Context) > 0 {
			_ = json.Unmarshal(e.Context, &context)
		}
		if string(context["geoip_disable"]) == "true" {
			continue
		}
		var geoip map[string]json.RawMessage
		if raw, exists := context["geoip"]; exists && json.Unmarshal(raw, &geoip) != nil {
			continue
		}
		if country == nil {
			if !client.IsValid() {
				client, _ = s.clientIP(r)
			}
			if !client.IsGlobalUnicast() || client.IsPrivate() || sharedAddressSpace.Contains(client) || s.trustedProxy(client) {
				return
			}
			code := s.geoIPCountry(client)
			if code == "" {
				return
			}
			country, _ = json.Marshal(code)
		}
		if context == nil {
			_ = json.Unmarshal(lakeapi.EventWithMetadata(*e).Context, &context)
		}
		if geoip == nil {
			geoip = map[string]json.RawMessage{}
		}
		geoip["country_code"] = country
		context["geoip"], _ = json.Marshal(geoip)
		if raw, err := json.Marshal(context); err == nil && len(raw) <= 64<<10 {
			e.Context = raw
		}
	}
}
