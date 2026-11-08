package porcupine

import (
	"errors"
	"net"
	"net/url"
	"strings"

	"golang.org/x/time/rate"
)

// OAuthConfig is operator-controlled machine-to-machine authorization configuration.
// A nil configuration retains legacy key authentication for migration/offline use.
type OAuthConfig struct {
	Issuer                     string
	AccessTokenLifetimeSeconds int
	AllowLegacyKeys            bool
}

type oauthRuntime struct {
	config      OAuthConfig
	issuer      string
	apiResource string
	mcpResource string
	limiter     *rate.Limiter
}

func normalizeOAuthConfig(c OAuthConfig) (*oauthRuntime, error) {
	u, err := url.Parse(c.Issuer)
	if err != nil || u == nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Path != "" && u.Path != "/" || u.RawPath != "" {
		return nil, errors.New("invalid OAuth issuer")
	}
	if u.Scheme != "https" {
		ip := net.ParseIP(u.Hostname())
		if u.Scheme != "http" || !(u.Hostname() == "localhost" || ip != nil && ip.IsLoopback()) {
			return nil, errors.New("OAuth issuer requires HTTPS except on loopback")
		}
	}
	if c.AccessTokenLifetimeSeconds == 0 {
		c.AccessTokenLifetimeSeconds = 900
	}
	if c.AccessTokenLifetimeSeconds < 60 || c.AccessTokenLifetimeSeconds > 3600 {
		return nil, errors.New("OAuth token lifetime must be 60 to 3600 seconds")
	}
	c.Issuer = strings.TrimSuffix(c.Issuer, "/")
	return &oauthRuntime{config: c, issuer: c.Issuer, apiResource: c.Issuer + "/api/v1", mcpResource: c.Issuer + "/mcp", limiter: rate.NewLimiter(5, 10)}, nil
}
