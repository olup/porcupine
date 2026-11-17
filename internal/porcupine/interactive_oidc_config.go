package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

// OIDCConfig selects the trusted upstream provider; clients cannot override it.
type OIDCConfig struct {
	Issuer       string
	ClientID     string
	ClientSecret string
}

type interactiveOIDC struct {
	provider *oidc.Provider
	verifier *oidc.IDTokenVerifier
	client   *http.Client
	oauth    oauth2.Config
	issuer   string
	*oauthBrowser
}

func newOAuthBrowser(secure bool) *oauthBrowser {
	return &oauthBrowser{secure: secure, sessions: map[string]*oidcBrowserSession{}}
}

type oauthBrowser struct {
	sharedDB      *sql.DB
	emailDelivery emailDeliveryWindow
	mu            sync.Mutex
	sessions      map[string]*oidcBrowserSession
	secure        bool
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

func newInteractiveOIDC(config *OIDCConfig, issuer string) (*interactiveOIDC, error) {
	if config == nil {
		return nil, nil
	}
	if !oidcEndpoint(config.Issuer) || strings.ContainsAny(config.Issuer, "?#") || config.ClientID == "" || len(config.ClientID) > 512 || config.ClientSecret == "" || len(config.ClientSecret) > 16<<10 || strings.ContainsAny(config.ClientSecret, "\x00\r\n\t") {
		return nil, errors.New("OIDC requires issuer, client ID and client secret")
	}
	issuerURL, err := url.Parse(issuer)
	if err != nil || issuerURL.Scheme != "https" && issuerURL.Scheme != "http" {
		return nil, errors.New("invalid OIDC callback issuer")
	}
	runtime := &interactiveOIDC{issuer: config.Issuer, oauthBrowser: newOAuthBrowser(issuerURL.Scheme == "https")}
	runtime.client = &http.Client{Transport: oidcTransport{base: http.DefaultTransport}, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("OIDC HTTP redirects are disabled") }}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
	runtime.provider = provider
	runtime.verifier = provider.Verifier(&oidc.Config{ClientID: config.ClientID})
	runtime.oauth = oauth2.Config{ClientID: config.ClientID, ClientSecret: config.ClientSecret, Endpoint: endpoint, RedirectURL: issuer + "/oauth/oidc/callback", Scopes: []string{oidc.ScopeOpenID, oidc.ScopeEmail, oidc.ScopeProfile}}
	return runtime, nil
}
