package porcupine

import (
	"errors"
	"net"
	"net/url"
	"strconv"
	"strings"

	"golang.org/x/time/rate"
)

// OAuthConfig is operator-controlled machine-to-machine authorization configuration.
// A nil configuration retains legacy key authentication for migration/offline use.
type OAuthConfig struct {
	Issuer                     string
	AccessTokenLifetimeSeconds int
	AllowLegacyKeys            bool
	OIDC                       *OIDCConfig
}

type oauthRuntime struct {
	config            OAuthConfig
	issuer            string
	apiResource       string
	mcpResource       string
	limiter           *rate.Limiter
	oidc              *interactiveOIDC
	authorizationGate oauthAuthorizationGate
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
	host := strings.ToLower(u.Hostname())
	if ip := net.ParseIP(host); ip != nil {
		host = ip.String()
	}
	port := u.Port()
	if port != "" {
		number, err := strconv.Atoi(port)
		if err != nil || number < 1 || number > 65535 {
			return nil, errors.New("invalid OAuth issuer port")
		}
		port = strconv.Itoa(number)
		if u.Scheme == "https" && port == "443" || u.Scheme == "http" && port == "80" {
			port = ""
		}
	}
	if port != "" {
		host = net.JoinHostPort(host, port)
	} else if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	canonical := u.Scheme + "://" + host
	if strings.TrimSuffix(c.Issuer, "/") != canonical {
		return nil, errors.New("OAuth issuer must use a canonical origin: lowercase scheme and host, canonical IP and port, no default port")
	}
	if c.AccessTokenLifetimeSeconds == 0 {
		c.AccessTokenLifetimeSeconds = 900
	}
	if c.AccessTokenLifetimeSeconds < 60 || c.AccessTokenLifetimeSeconds > 3600 {
		return nil, errors.New("OAuth token lifetime must be 60 to 3600 seconds")
	}
	c.Issuer = canonical
	interactive, err := newInteractiveOIDC(c.OIDC, c.Issuer)
	if err != nil {
		return nil, err
	}
	return &oauthRuntime{config: c, issuer: c.Issuer, apiResource: c.Issuer + "/api/v1", mcpResource: c.Issuer + "/mcp", limiter: rate.NewLimiter(5, 10), oidc: interactive}, nil
}
