package porcupine

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"testing"
)

func TestWebhookEndpointPublicHTTPSOnly(t *testing.T) {
	for _, endpoint := range []string{"http://example.com", "https://127.0.0.1", "https://169.254.169.254/latest", "https://10.1.2.3", "https://100.100.100.200", "https://[::ffff:127.0.0.1]", "https://[64:ff9b::a9fe:a9fe]", "https://[2001:db8::1]", "https://metadata.google.internal", "https://localhost", "https://user:password@example.com", "https://example.com/#fragment", "https://example.com:0", "https://example.com:65536"} {
		if validateWebhookEndpoint(endpoint) == nil {
			t.Fatal("unsafe endpoint accepted")
		}
	}
	for _, endpoint := range []string{"https://example.com/path?secret=synthetic", "https://8.8.8.8/hook", "https://[2606:4700:4700::1111]/hook"} {
		if err := validateWebhookEndpoint(endpoint); err != nil {
			t.Fatal("public HTTPS rejected", err)
		}
	}
	client := newWebhookHTTPClient()
	defer client.CloseIdleConnections()
	transport := client.Transport.(*http.Transport)
	if transport.Proxy != nil || !transport.DisableKeepAlives || client.CheckRedirect(&http.Request{}, nil) != http.ErrUseLastResponse {
		t.Fatal("unsafe transport defaults")
	}
}
func TestWebhookDNSPinnedAndMixedAnswersBlocked(t *testing.T) {
	calls := 0
	dial := func(_ context.Context, network, address string) (net.Conn, error) {
		calls++
		if address != "8.8.8.8:443" {
			t.Fatal("hostname or unchecked IP reached dial")
		}
		return nil, errors.New("synthetic failure")
	}
	for _, ips := range [][]netip.Addr{{netip.MustParseAddr("127.0.0.1")}, {netip.MustParseAddr("8.8.8.8"), netip.MustParseAddr("10.0.0.1")}} {
		_, err := dialWebhook(context.Background(), "tcp", "public.example:443", func(context.Context, string, string) ([]netip.Addr, error) { return ips, nil }, dial)
		if err == nil || calls != 0 {
			t.Fatal("private DNS answer dialed")
		}
	}
	_, err := dialWebhook(context.Background(), "tcp", "public.example:443", func(_ context.Context, _ string, host string) ([]netip.Addr, error) {
		if host != "public.example" {
			t.Fatal("wrong resolver hostname")
		}
		return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
	}, dial)
	if err == nil || calls != 1 {
		t.Fatal("numeric public pin not dialed exactly once")
	}
}
func TestWebhookHeadersRejectRoutingAndSmuggling(t *testing.T) {
	for _, headers := range []map[string]string{{"Host": "example.com"}, {"X-Forwarded-Host": "internal"}, {"Authorization": "Basic synthetic"}, {"X-Key": "one\r\ntwo"}, {"X-Key": "one", "x-key": "two"}, {"X-Porcupine-Delivery-ID": "override"}, {"Idempotency-Key": "override"}} {
		if validateWebhookHeaders(headers) == nil {
			t.Fatal("unsafe headers accepted")
		}
	}
	if err := validateWebhookHeaders(map[string]string{"Authorization": "Bearer synthetic", "X-Api-Key": "synthetic"}); err != nil {
		t.Fatal(err)
	}
}
