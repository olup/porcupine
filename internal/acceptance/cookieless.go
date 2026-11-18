package acceptance

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"net/netip"

	eventapi "github.com/olup/porcupine/internal/event"
	"github.com/olup/porcupine/internal/ingest"
)

func CookielessDistinctID(secret []byte, project string, ip netip.Addr, userAgent string) string {
	mac := hmac.New(sha256.New, secret)
	for _, value := range []string{project, ip.String(), userAgent} {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		mac.Write(length[:])
		mac.Write([]byte(value))
	}
	return "cookieless_" + hex.EncodeToString(mac.Sum(nil))
}

func CookielessProperties(raw json.RawMessage, id, day string) json.RawMessage {
	var properties map[string]any
	// These properties were decoded and marshaled during request validation.
	if err := json.Unmarshal(raw, &properties); err != nil {
		panic(err)
	}
	for _, key := range []string{"$device_id", "$user_id", "$anon_distinct_id", "$session_id", "$window_id", "$ip", "ip", "ip_address", "client_ip", "remote_addr", "x-forwarded-for"} {
		delete(properties, key)
	}
	properties["distinct_id"] = id
	properties["cookieless"] = true
	properties["identity_day"] = day
	result, err := json.Marshal(properties)
	if err != nil {
		panic(err)
	}
	return result
}

func ResolveCookieless(event eventapi.Event, id, day string) eventapi.Event {
	event.DistinctID = id
	event.Properties = CookielessProperties(event.Properties, id, day)
	if event.SourceID != "" {
		event.Context = ingest.ScrubContext(event.Context)
	}
	return event
}
