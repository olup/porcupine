package porcupine

import (
	"context"
	"errors"
	"strings"

	"github.com/olup/porcupine/internal/auth"
	"golang.org/x/time/rate"
)

// EmailConfig configures invited human accounts and optional SMTP sign-in delivery.
type EmailConfig struct {
	SendCode       func(context.Context, string, string) error
	SendInvitation func(ctx context.Context, email string, instanceAdmin bool) error
	AdminEmail     string
}

// OAuthConfig is operator-controlled authorization configuration.
// A nil configuration retains legacy key authentication for migration/offline use.
type OAuthConfig struct {
	Issuer                         string
	AccessTokenLifetimeSeconds     int
	AllowLegacyKeys                bool
	AllowRestoredIdentityBootstrap bool
	OIDC                           *OIDCConfig
	Email                          *EmailConfig
}

type oauthRuntime struct {
	config            OAuthConfig
	issuer            string
	apiResource       string
	mcpResource       string
	limiter           *rate.Limiter
	oidc              *interactiveOIDC
	browser           *oauthBrowser
	authorizationGate oauthAuthorizationGate
}

func normalizeOAuthConfig(c OAuthConfig) (*oauthRuntime, error) {
	issuer, err := auth.NormalizeIssuer(c.Issuer, c.OIDC != nil || c.Email != nil)
	if err != nil {
		return nil, err
	}
	c.Issuer = issuer
	if c.OIDC != nil && c.Email == nil {
		c.Email = &EmailConfig{}
	}
	if c.Email != nil && c.Email.SendCode == nil && c.OIDC == nil {
		return nil, errors.New("email login requires a configured sender")
	}
	if c.AccessTokenLifetimeSeconds == 0 {
		c.AccessTokenLifetimeSeconds = 900
	}
	if c.AccessTokenLifetimeSeconds < 60 || c.AccessTokenLifetimeSeconds > 3600 {
		return nil, errors.New("OAuth token lifetime must be 60 to 3600 seconds")
	}
	interactive, err := newInteractiveOIDC(c.OIDC, c.Issuer)
	if err != nil {
		return nil, err
	}
	var browser *oauthBrowser
	if interactive != nil {
		browser = interactive.oauthBrowser
	} else if c.Email != nil {
		browser = newOAuthBrowser(strings.HasPrefix(c.Issuer, "https://"))
	}
	return &oauthRuntime{browser: browser, config: c, issuer: c.Issuer, apiResource: c.Issuer + "/api/v1", mcpResource: c.Issuer + "/mcp", limiter: rate.NewLimiter(5, 10), oidc: interactive}, nil
}
