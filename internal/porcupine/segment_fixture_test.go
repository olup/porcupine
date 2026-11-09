package porcupine

import (
	"encoding/json"
	"strings"
	"testing"
)

// Captured from pinned @segment/analytics-node 3.1.0 over real local HTTP.
// The synthetic write key and batch send time are normalized.
const segmentAnalyticsNode310Fixture = `{
  "batch": [
    {
      "timestamp": "2025-03-04T05:06:07.123Z",
      "integrations": {},
      "type": "identify",
      "traits": {
        "campaign": "docs"
      },
      "context": {
        "library": {
          "name": "@segment/analytics-node",
          "version": "3.1.0"
        }
      },
      "anonymousId": "anonymous-device",
      "messageId": "segment-sdk-anon",
      "_metadata": {
        "nodeVersion": "v22.22.0",
        "jsRuntime": "node"
      }
    },
    {
      "timestamp": "2025-03-04T05:06:07.123Z",
      "integrations": {},
      "event": "Before Login",
      "type": "track",
      "properties": {
        "token": "business-token",
        "nested": {
          "unicode": "東京 🦔",
          "values": [
            1,
            true,
            null
          ]
        }
      },
      "context": {
        "locale": "fr-FR",
        "library": {
          "name": "@segment/analytics-node",
          "version": "3.1.0"
        }
      },
      "anonymousId": "anonymous-device",
      "messageId": "segment-sdk-before",
      "_metadata": {
        "nodeVersion": "v22.22.0",
        "jsRuntime": "node"
      }
    },
    {
      "timestamp": "2025-03-04T05:06:07.123Z",
      "integrations": {},
      "type": "identify",
      "userId": "account-7",
      "traits": {
        "plan": "pro"
      },
      "context": {
        "library": {
          "name": "@segment/analytics-node",
          "version": "3.1.0"
        }
      },
      "anonymousId": "anonymous-device",
      "messageId": "segment-sdk-login",
      "_metadata": {
        "nodeVersion": "v22.22.0",
        "jsRuntime": "node"
      }
    },
    {
      "timestamp": "2025-03-04T05:06:07.123Z",
      "integrations": {},
      "type": "page",
      "properties": {
        "url": "https://example.test/pricing",
        "title": "Pricing",
        "name": "business-name",
        "category": "Docs"
      },
      "category": "Docs",
      "name": "Pricing",
      "context": {
        "library": {
          "name": "@segment/analytics-node",
          "version": "3.1.0"
        }
      },
      "userId": "account-7",
      "messageId": "segment-sdk-page",
      "_metadata": {
        "nodeVersion": "v22.22.0",
        "jsRuntime": "node"
      }
    },
    {
      "timestamp": "2025-03-04T05:06:07.123Z",
      "integrations": {
        "Vendor": {
          "apiKey": "synthetic-vendor-secret"
        }
      },
      "event": "After Login",
      "type": "track",
      "properties": {
        "revenue": 42.25
      },
      "context": {
        "library": {
          "name": "@segment/analytics-node",
          "version": "3.1.0"
        }
      },
      "userId": "account-7",
      "messageId": "segment-sdk-after",
      "_metadata": {
        "nodeVersion": "v22.22.0",
        "jsRuntime": "node"
      }
    }
  ],
  "writeKey": "FIXTURE_WRITE_KEY",
  "sentAt": "2025-03-04T05:06:08.000Z"
}`

func TestSegmentRecordedAnalyticsNode310Batch(t *testing.T) {
	s := service(t, 20)
	projectID, key := project(t, s)
	payload := []byte(strings.ReplaceAll(segmentAnalyticsNode310Fixture, "FIXTURE_WRITE_KEY", key))
	first := transportRequest(t, s, "POST", "/v1/batch", payload, segmentHeaders(key), 200)
	var ack map[string]any
	json.Unmarshal(first.Body.Bytes(), &ack)
	if ack["accepted"] != float64(5) || ack["identity_ignored"] != float64(0) {
		t.Fatalf("unexpected SDK acknowledgement: %v", ack)
	}
	before := segmentEvent(t, s, projectID, "segment-sdk-before")
	if before.DistinctID != "anonymous-device" || !strings.Contains(string(before.Properties), `"token":"business-token"`) {
		t.Fatal("SDK ordinary event changed")
	}
	traits, identified, _ := storedIdentityProfile(t, s, projectID, "account-7")
	if !identified || traits["campaign"] != "docs" || traits["plan"] != "pro" {
		t.Fatal("SDK traits did not link")
	}
	events := request(t, s, "GET", "/api/v1/projects/"+projectID+"/events", adminToken, "", 200)["events"].([]any)
	if len(events) != 3 {
		t.Fatal("SDK identity controls appeared as ordinary events")
	}
	for _, raw := range events {
		event := raw.(map[string]any)
		encoded, _ := json.Marshal(event)
		if strings.Contains(string(encoded), "synthetic-vendor-secret") || strings.Contains(string(encoded), key) {
			t.Fatal("SDK routing configuration or credential persisted")
		}
	}
	retry := transportRequest(t, s, "POST", "/v1/batch", payload, segmentHeaders(key), 200)
	json.Unmarshal(retry.Body.Bytes(), &ack)
	if ack["accepted"] != float64(0) || ack["duplicates"] != float64(5) {
		t.Fatal("SDK retry did not deduplicate")
	}
}
