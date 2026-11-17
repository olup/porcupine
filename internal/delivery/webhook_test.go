package delivery

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

type webhookTransport func(*http.Request) (*http.Response, error)

func (f webhookTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestSendWebhookHeadersAndBoundedResponse(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		status, size int
		code         string
	}{{"success", 204, 0, ""}, {"rejected", 429, 1, "http_rejected"}, {"oversized", 200, (64 << 10) + 1, "invalid_response"}} {
		t.Run(tc.name, func(t *testing.T) {
			client := &http.Client{Transport: webhookTransport(func(r *http.Request) (*http.Response, error) {
				body, _ := io.ReadAll(r.Body)
				if r.Method != "POST" || r.URL.String() != "https://example.com/hook" || r.Header.Get("Idempotency-Key") != "delivery-one" || r.Header.Get("X-Porcupine-Delivery-ID") != "delivery-one" || r.Header.Get("Authorization") != "Bearer synthetic" || r.Header.Get("Content-Type") != "application/json" || string(body) != "{}" {
					t.Fatal("webhook contract changed")
				}
				return &http.Response{StatusCode: tc.status, Body: io.NopCloser(strings.NewReader(strings.Repeat("x", tc.size)))}, nil
			})}
			status, code := SendWebhook(context.Background(), client, WebhookMessage{Endpoint: "https://example.com/hook", DeliveryID: "delivery-one", Payload: "{}", Headers: map[string]string{"Authorization": "Bearer synthetic"}})
			if status != tc.status || code != tc.code {
				t.Fatalf("got %d %s", status, code)
			}
		})
	}
}
