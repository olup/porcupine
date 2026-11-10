package porcupine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

// OIDCConfig is upstream operator configuration. Bindings never come from a client.
type OIDCConfig struct {
	Issuer       string
	ClientID     string
	ClientSecret string
	Bindings     []OIDCBinding
}

type OIDCBinding struct {
	Subject     string   `json:"subject"`
	Permissions []string `json:"permissions"`
	Projects    []string `json:"projects"`
}

type interactiveOIDC struct {
	provider *oidc.Provider
	verifier *oidc.IDTokenVerifier
	client   *http.Client
	oauth    oauth2.Config
	issuer   string
	bindings map[string]actor
	*oauthBrowser
}

func newOAuthBrowser(secure bool) *oauthBrowser {
	return &oauthBrowser{secure: secure, sessions: map[string]*oidcBrowserSession{}}
}

type oauthBrowser struct {
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
	if !oidcEndpoint(config.Issuer) || strings.ContainsAny(config.Issuer, "?#") || config.ClientID == "" || len(config.ClientID) > 512 || config.ClientSecret == "" {
		return nil, errors.New("OIDC requires issuer, client ID and client secret")
	}
	if len(config.Bindings) > 10000 {
		return nil, errors.New("OIDC allows at most 10000 explicit subject bindings")
	}
	issuerURL, err := url.Parse(issuer)
	if err != nil || issuerURL.Scheme != "https" && issuerURL.Scheme != "http" {
		return nil, errors.New("invalid OIDC callback issuer")
	}
	runtime := &interactiveOIDC{issuer: config.Issuer, bindings: map[string]actor{}, oauthBrowser: newOAuthBrowser(issuerURL.Scheme == "https")}
	for _, binding := range config.Bindings {
		if binding.Subject == "" || len(binding.Subject) > 512 || len(binding.Permissions) == 0 || len(binding.Projects) == 0 || len(binding.Projects) > 1000 {
			return nil, errors.New("invalid OIDC binding")
		}
		if _, exists := runtime.bindings[binding.Subject]; exists {
			return nil, errors.New("duplicate OIDC subject binding")
		}
		for _, p := range binding.Permissions {
			if !slices.Contains(validPermissions, p) || p == "instance:admin" {
				return nil, errors.New("OIDC bindings require explicit non-admin permissions")
			}
		}
		for _, p := range binding.Projects {
			if p == "" || p == "*" || len(p) > 256 {
				return nil, errors.New("OIDC bindings require explicit project IDs")
			}
		}
		runtime.bindings[binding.Subject] = actor{Permissions: slices.Clone(binding.Permissions), Projects: slices.Clone(binding.Projects)}
	}
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
	runtime.oauth = oauth2.Config{ClientID: config.ClientID, ClientSecret: config.ClientSecret, Endpoint: endpoint, RedirectURL: issuer + "/oauth/oidc/callback", Scopes: []string{oidc.ScopeOpenID}}
	return runtime, nil
}
