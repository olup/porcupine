package auth

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

// OIDCConfig selects the trusted upstream provider, independent of local accounts.
type OIDCConfig struct{ Issuer, ClientID, ClientSecret string }

type OIDCProvider struct {
	verifier *oidc.IDTokenVerifier
	client   *http.Client
	oauth    oauth2.Config
	issuer   string
}

type oidcResponseBody struct {
	io.Reader
	io.Closer
}
type oidcTransport struct{ base http.RoundTripper }

func (transport oidcTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := transport.base.RoundTrip(request)
	if err == nil {
		response.Body = oidcResponseBody{Reader: io.LimitReader(response.Body, 1<<20), Closer: response.Body}
	}
	return response, err
}

func oidcEndpoint(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || u.Fragment != "" {
		return false
	}
	ip := net.ParseIP(u.Hostname())
	return u.Scheme == "https" || u.Scheme == "http" && (u.Hostname() == "localhost" || ip != nil && ip.IsLoopback())
}

// ValidateOIDCConfig checks local settings without contacting the provider.
func ValidateOIDCConfig(config OIDCConfig, issuer string) error {
	if !oidcEndpoint(config.Issuer) || strings.ContainsAny(config.Issuer, "?#") || config.ClientID == "" || len(config.ClientID) > 512 || config.ClientSecret == "" || len(config.ClientSecret) > 16<<10 || strings.ContainsAny(config.ClientSecret, "\x00\r\n\t") {
		return errors.New("OIDC requires issuer, client ID and client secret")
	}
	issuerURL, err := url.Parse(issuer)
	if err != nil || issuerURL.Scheme != "https" && issuerURL.Scheme != "http" {
		return errors.New("invalid OIDC callback issuer")
	}
	return nil
}

func NewOIDCProvider(ctx context.Context, config OIDCConfig, issuer string) (*OIDCProvider, error) {
	if err := ValidateOIDCConfig(config, issuer); err != nil {
		return nil, err
	}
	runtime := &OIDCProvider{issuer: config.Issuer}
	runtime.client = &http.Client{Transport: oidcTransport{base: http.DefaultTransport}, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("OIDC HTTP redirects are disabled") }}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ctx = oidc.ClientContext(ctx, runtime.client)
	provider, err := oidc.NewProvider(ctx, config.Issuer)
	if err != nil {
		return nil, fmt.Errorf("OIDC discovery failed: %w", err)
	}
	endpoint := provider.Endpoint()
	if !oidcEndpoint(endpoint.AuthURL) || !oidcEndpoint(endpoint.TokenURL) {
		return nil, errors.New("OIDC discovery contains insecure endpoints")
	}
	var discovery struct {
		JWKS string `json:"jwks_uri"`
	}
	if provider.Claims(&discovery) != nil || !oidcEndpoint(discovery.JWKS) {
		return nil, errors.New("OIDC discovery contains insecure JWKS endpoint")
	}
	runtime.verifier = provider.Verifier(&oidc.Config{ClientID: config.ClientID})
	runtime.oauth = oauth2.Config{ClientID: config.ClientID, ClientSecret: config.ClientSecret, Endpoint: endpoint, RedirectURL: issuer + "/oauth/oidc/callback", Scopes: []string{oidc.ScopeOpenID, oidc.ScopeEmail, oidc.ScopeProfile}}
	return runtime, nil
}

type OIDCClaims struct {
	AuthorizedParty string `json:"azp"`
	Email           string `json:"email"`
	EmailVerified   bool   `json:"email_verified"`
	HostedDomain    string `json:"hd"`
}

func VerifiedOIDCEmail(issuer string, claims OIDCClaims) (string, error) {
	if !claims.EmailVerified {
		return "", errors.New("identity provider did not verify email")
	}
	email, err := NormalizeEmail(claims.Email)
	if err != nil {
		return "", errors.New("identity provider did not provide a valid email")
	}
	if issuer == "https://accounts.google.com" {
		domain := email[strings.LastIndexByte(email, '@')+1:]
		if domain != "gmail.com" && claims.HostedDomain == "" {
			return "", errors.New("Google does not establish current email ownership; use email sign-in")
		}
	}
	return email, nil
}

// Identity is a verified provider assertion. It carries no Porcupine authority.
type Identity struct{ Subject, Email string }

var ErrOIDCEmail = errors.New("a verified invited email is required")

func (p *OIDCProvider) Issuer() string { return p.issuer }

func (p *OIDCProvider) AuthorizationURL(state, nonce, verifier string) string {
	return p.oauth.AuthCodeURL(state, oidc.Nonce(nonce), oauth2.S256ChallengeOption(verifier))
}

func (p *OIDCProvider) ExchangeIdentity(ctx context.Context, code, verifier, nonce string) (Identity, error) {
	ctx = oidc.ClientContext(ctx, p.client)
	token, err := p.oauth.Exchange(ctx, code, oauth2.VerifierOption(verifier))
	if err != nil {
		return Identity{}, err
	}
	raw, ok := token.Extra("id_token").(string)
	if !ok || len(raw) > 65536 {
		return Identity{}, errors.New("invalid OIDC identity token")
	}
	identity, err := p.verifier.Verify(ctx, raw)
	if err != nil {
		return Identity{}, err
	}
	if identity.Subject == "" || len(identity.Subject) > 512 || subtle.ConstantTimeCompare([]byte(identity.Nonce), []byte(nonce)) != 1 {
		return Identity{}, errors.New("invalid OIDC subject or nonce")
	}
	var claims OIDCClaims
	if identity.Claims(&claims) != nil || claims.AuthorizedParty != "" && claims.AuthorizedParty != p.oauth.ClientID || len(identity.Audience) > 1 && claims.AuthorizedParty != p.oauth.ClientID {
		return Identity{}, errors.New("invalid OIDC authorized party")
	}
	email, err := VerifiedOIDCEmail(p.issuer, claims)
	if err != nil {
		return Identity{}, fmt.Errorf("%w: %v", ErrOIDCEmail, err)
	}
	return Identity{Subject: identity.Subject, Email: email}, nil
}
