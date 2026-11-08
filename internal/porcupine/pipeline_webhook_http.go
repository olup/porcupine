package porcupine

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"
)

var errWebhookEndpoint = errors.New("webhook destination must use a public HTTPS endpoint")
var blockedWebhookNetworks = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"), netip.MustParsePrefix("168.63.129.16/32"), netip.MustParsePrefix("::/96"), netip.MustParsePrefix("100.64.0.0/10"), netip.MustParsePrefix("192.0.0.0/24"), netip.MustParsePrefix("192.0.2.0/24"), netip.MustParsePrefix("192.88.99.0/24"), netip.MustParsePrefix("198.18.0.0/15"), netip.MustParsePrefix("198.51.100.0/24"), netip.MustParsePrefix("203.0.113.0/24"), netip.MustParsePrefix("240.0.0.0/4"), netip.MustParsePrefix("64:ff9b::/96"), netip.MustParsePrefix("64:ff9b:1::/48"), netip.MustParsePrefix("100::/64"), netip.MustParsePrefix("2001::/23"), netip.MustParsePrefix("2001:db8::/32"), netip.MustParsePrefix("2002::/16"),
}

func webhookPublicIP(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsValid() || !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.Zone() != "" {
		return false
	}
	for _, prefix := range blockedWebhookNetworks {
		if prefix.Contains(ip) {
			return false
		}
	}
	return true
}
func validateWebhookEndpoint(raw string) error {
	if len(raw) == 0 || len(raw) > 4096 || strings.TrimSpace(raw) != raw {
		return errWebhookEndpoint
	}
	endpoint, err := url.Parse(raw)
	if err != nil || endpoint.Scheme != "https" || endpoint.Opaque != "" || endpoint.Hostname() == "" || endpoint.User != nil || endpoint.Fragment != "" || strings.ContainsAny(raw, "\\\r\n\t") {
		return errWebhookEndpoint
	}
	host := strings.TrimSuffix(strings.ToLower(endpoint.Hostname()), ".")
	if ip, err := netip.ParseAddr(host); err == nil {
		if !webhookPublicIP(ip) {
			return errWebhookEndpoint
		}
	} else {
		if !strings.Contains(host, ".") || strings.HasSuffix(host, ".localhost") || strings.HasSuffix(host, ".local") || strings.HasSuffix(host, ".internal") {
			return errWebhookEndpoint
		}
		if len(host) > 253 {
			return errWebhookEndpoint
		}
		for _, label := range strings.Split(host, ".") {
			if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
				return errWebhookEndpoint
			}
			for _, ch := range label {
				if !(ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9' || ch == '-') {
					return errWebhookEndpoint
				}
			}
		}
	}
	if port := endpoint.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return errWebhookEndpoint
		}
	}
	return nil
}
func validateWebhookHeaders(headers map[string]string) error {
	if len(headers) > 16 {
		return errors.New("too many webhook headers")
	}
	total := 0
	seen := map[string]bool{}
	for name, value := range headers {
		lower := strings.ToLower(name)
		if name == "" || seen[lower] || len(name) > 100 || len(value) > 4096 || strings.ContainsAny(value, "\r\n\x00") || strings.TrimSpace(value) != value {
			return errors.New("invalid webhook headers")
		}
		for _, ch := range name {
			if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '-') {
				return errors.New("invalid webhook headers")
			}
		}
		if lower == "authorization" {
			if !strings.HasPrefix(value, "Bearer ") || strings.TrimSpace(strings.TrimPrefix(value, "Bearer ")) == "" || strings.ContainsAny(strings.TrimPrefix(value, "Bearer "), " \t") {
				return errors.New("webhook authorization must use Bearer")
			}
		} else if !strings.HasPrefix(lower, "x-") || strings.HasPrefix(lower, "x-porcupine-") || strings.HasPrefix(lower, "x-forwarded-") || lower == "x-http-method-override" || lower == "x-original-url" || lower == "x-rewrite-url" {
			return errors.New("unsupported webhook header")
		}
		total += len(name) + len(value)
		seen[lower] = true
	}
	if total > 8192 {
		return errors.New("webhook headers exceed limit")
	}
	return nil
}

type webhookLookup func(context.Context, string, string) ([]netip.Addr, error)
type webhookDial func(context.Context, string, string) (net.Conn, error)

func dialWebhook(ctx context.Context, network, address string, lookup webhookLookup, dial webhookDial) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, errWebhookEndpoint
	}
	ips, err := lookup(ctx, "ip", host)
	if err != nil || len(ips) == 0 || len(ips) > 32 {
		return nil, errWebhookEndpoint
	}
	// Reject mixed DNS answers rather than choosing a public member of a private
	// destination. Dial the verified numeric address; TLS still uses the URL host.
	for _, ip := range ips {
		if !webhookPublicIP(ip) {
			return nil, errWebhookEndpoint
		}
	}
	for _, ip := range ips {
		connection, err := dial(ctx, network, net.JoinHostPort(ip.String(), port))
		if err == nil {
			return connection, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	return nil, errors.New("webhook connection failed")
}
func newWebhookHTTPClient() *http.Client {
	dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: -1}
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true, ForceAttemptHTTP2: false, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}, TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 10 * time.Second, MaxResponseHeaderBytes: 16 << 10, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		return dialWebhook(ctx, network, address, net.DefaultResolver.LookupNetIP, dialer.DialContext)
	}}
	return &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }, Timeout: 15 * time.Second}
}
