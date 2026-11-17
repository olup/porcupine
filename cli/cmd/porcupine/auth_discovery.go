package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"
)

type oauthProvider struct {
	Issuer                string `json:"issuer"`
	AuthorizationEndpoint string `json:"authorization_endpoint"`
	TokenEndpoint         string `json:"token_endpoint"`
	External              bool   `json:"external"`
}

type oauthMetadataStatus int

func (s oauthMetadataStatus) Error() string {
	return fmt.Sprintf("OAuth metadata endpoint returned HTTP %d", s)
}

func builtinProvider(origin string) oauthProvider {
	return oauthProvider{Issuer: origin, AuthorizationEndpoint: origin + "/oauth/authorize", TokenEndpoint: origin + "/oauth/token"}
}

func secureOAuthURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || u.User != nil || strings.ContainsAny(raw, "?#") {
		return nil, errors.New("OAuth metadata requires absolute HTTPS URLs without credentials, query or fragment")
	}
	ip := net.ParseIP(u.Hostname())
	local := strings.EqualFold(u.Hostname(), "localhost") || ip != nil && ip.IsLoopback()
	if u.Scheme != "https" && !(u.Scheme == "http" && local) {
		return nil, errors.New("OAuth metadata requires HTTPS or an HTTP loopback address")
	}
	return u, nil
}

func (p oauthProvider) validate() error {
	issuer, err := secureOAuthURL(p.Issuer)
	if err != nil {
		return err
	}
	for _, raw := range []string{p.AuthorizationEndpoint, p.TokenEndpoint} {
		u, err := secureOAuthURL(raw)
		if err != nil || u.Scheme != issuer.Scheme || !strings.EqualFold(u.Host, issuer.Host) {
			return errors.New("OAuth endpoints must belong to the discovered issuer origin")
		}
	}
	return nil
}

func oauthMetadata(ctx context.Context, address string, out any) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(request)
	if err != nil {
		return errors.New("could not reach OAuth metadata endpoint")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return oauthMetadataStatus(response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, (64<<10)+1))
	if err != nil || len(body) > 64<<10 || json.Unmarshal(body, out) != nil {
		return errors.New("invalid OAuth metadata response")
	}
	return nil
}

func discoverProvider(ctx context.Context, origin string) (oauthProvider, error) {
	var resource struct {
		Resource             string   `json:"resource"`
		AuthorizationServers []string `json:"authorization_servers"`
		BearerMethods        []string `json:"bearer_methods_supported"`
	}
	if err := oauthMetadata(ctx, origin+"/.well-known/oauth-protected-resource/api/v1", &resource); err != nil {
		return oauthProvider{}, err
	}
	if resource.Resource != origin+"/api/v1" || len(resource.AuthorizationServers) != 1 || !slices.Contains(resource.BearerMethods, "header") {
		return oauthProvider{}, errors.New("OAuth metadata does not describe this instance's API resource")
	}
	issuer, err := secureOAuthURL(resource.AuthorizationServers[0])
	if err != nil {
		return oauthProvider{}, err
	}
	metadataURL := *issuer
	metadataURL.Path = "/.well-known/oauth-authorization-server" + strings.TrimSuffix(issuer.Path, "/")
	metadataURL.RawPath = ""
	var metadata struct {
		Issuer                string   `json:"issuer"`
		AuthorizationEndpoint string   `json:"authorization_endpoint"`
		TokenEndpoint         string   `json:"token_endpoint"`
		ResponseTypes         []string `json:"response_types_supported"`
		ChallengeMethods      []string `json:"code_challenge_methods_supported"`
		ResponseIssuer        bool     `json:"authorization_response_iss_parameter_supported"`
	}
	if err := oauthMetadata(ctx, metadataURL.String(), &metadata); err != nil {
		if err != oauthMetadataStatus(http.StatusNotFound) {
			return oauthProvider{}, err
		}
		if err := oauthMetadata(ctx, strings.TrimSuffix(issuer.String(), "/")+"/.well-known/openid-configuration", &metadata); err != nil {
			return oauthProvider{}, err
		}
	}
	if metadata.Issuer != resource.AuthorizationServers[0] || !slices.Contains(metadata.ResponseTypes, "code") || !slices.Contains(metadata.ChallengeMethods, "S256") || !metadata.ResponseIssuer {
		return oauthProvider{}, errors.New("OAuth issuer must support authorization code, PKCE S256 and issuer-bound callbacks")
	}
	provider := oauthProvider{Issuer: metadata.Issuer, AuthorizationEndpoint: metadata.AuthorizationEndpoint, TokenEndpoint: metadata.TokenEndpoint}
	provider.External = provider != builtinProvider(origin)
	if err := provider.validate(); err != nil {
		return oauthProvider{}, err
	}
	return provider, nil
}
