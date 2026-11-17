package porcupine

import (
	"context"
	"errors"

	"github.com/olup/porcupine/internal/auth"
	"golang.org/x/time/rate"
)

// EmailConfig configures invited human accounts and optional SMTP sign-in delivery.
type EmailConfig struct {
	SendCode       func(context.Context, string, string) error
	SendInvitation func(ctx context.Context, email string, instanceAdmin bool) error
	AdminEmail     string
}

// OIDCConfig selects the trusted upstream provider; clients cannot override it.
type OIDCConfig struct{ Issuer, ClientID, ClientSecret string }

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

// AuthConfig selects either the integrated identity provider or one trusted
// external access-token issuer. Native ingestion and machine keys work in both.
type AuthConfig struct {
	Mode      string
	PublicURL string
	External  auth.ExternalConfig
}

type oauthRuntime struct {
	config            OAuthConfig
	issuer            string
	limiter           *rate.Limiter
	browser           *auth.Login
	authorizationGate auth.AuthorizationGate
}

type resourceRuntime struct {
	publicURL   string
	apiResource string
	mcpResource string
	external    *auth.ExternalVerifier
}

func normalizeExternalRuntime(c AuthConfig) (*resourceRuntime, error) {
	public, err := auth.NormalizeIssuer(c.PublicURL, false)
	if err != nil {
		return nil, err
	}
	verifier, err := auth.NewExternalVerifier(c.External)
	if err != nil {
		return nil, err
	}
	return &resourceRuntime{publicURL: public, apiResource: public + "/api/v1", mcpResource: public + "/mcp", external: verifier}, nil
}

func (s *Service) externalAuthEnabled() bool {
	return s.resources != nil && s.resources.external != nil
}

// ValidateOAuthConfig checks startup settings without network requests or database writes.
func ValidateOAuthConfig(config OAuthConfig) error {
	_, err := normalizeOAuthSettings(config)
	return err
}

func normalizeOAuthSettings(c OAuthConfig) (OAuthConfig, error) {
	issuer, err := auth.NormalizeIssuer(c.Issuer, c.OIDC != nil || c.Email != nil)
	if err != nil {
		return OAuthConfig{}, err
	}
	c.Issuer = issuer
	if c.OIDC != nil && c.Email == nil {
		c.Email = &EmailConfig{}
	}
	if c.Email != nil && c.Email.SendCode == nil && c.OIDC == nil {
		return OAuthConfig{}, errors.New("email login requires a configured sender")
	}
	if c.AccessTokenLifetimeSeconds == 0 {
		c.AccessTokenLifetimeSeconds = 900
	}
	if c.AccessTokenLifetimeSeconds < 60 || c.AccessTokenLifetimeSeconds > 3600 {
		return OAuthConfig{}, errors.New("OAuth token lifetime must be 60 to 3600 seconds")
	}
	if c.OIDC != nil {
		if err := auth.ValidateOIDCConfig(auth.OIDCConfig{Issuer: c.OIDC.Issuer, ClientID: c.OIDC.ClientID, ClientSecret: c.OIDC.ClientSecret}, c.Issuer); err != nil {
			return OAuthConfig{}, err
		}
	}
	return c, nil
}

func normalizeOAuthConfig(c OAuthConfig) (*oauthRuntime, error) {
	c, err := normalizeOAuthSettings(c)
	if err != nil {
		return nil, err
	}
	var browser *auth.Login
	if c.OIDC != nil || c.Email != nil {
		loginConfig := auth.LoginConfig{Issuer: c.Issuer}
		if c.Email != nil {
			loginConfig.SendCode = c.Email.SendCode
		}
		if c.OIDC != nil {
			loginConfig.OIDC = &auth.OIDCConfig{Issuer: c.OIDC.Issuer, ClientID: c.OIDC.ClientID, ClientSecret: c.OIDC.ClientSecret}
		}
		browser, err = auth.NewLogin(context.Background(), loginConfig)
		if err != nil {
			return nil, err
		}
	}
	return &oauthRuntime{browser: browser, config: c, issuer: c.Issuer, limiter: rate.NewLimiter(5, 10)}, nil
}
