package porcupine

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"net"
	"net/url"
	"slices"
	"strings"
)

var interactiveOAuthScopes = []string{"events:read", "management:read"}

type oauthAuthorizationRequest struct {
	ID            string
	ClientID      string
	ClientName    string
	RedirectURI   string
	Scope         string
	Resource      string
	State         string
	CodeChallenge string
	ExpiresAtMS   int64
}

func (s *Service) interactiveOAuthEnabled() bool { return s.oauth != nil && s.oauth.config.OIDC != nil }

func oauthRandom(prefix string) (string, error) {
	var random [32]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	return prefix + base64.RawURLEncoding.EncodeToString(random[:]), nil
}

func validOAuthRedirect(raw string) bool {
	if len(raw) == 0 || len(raw) > 2048 || strings.ContainsAny(raw, "\r\n\x00*") {
		return false
	}
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || u.User != nil || u.Fragment != "" || u.RawFragment != "" || u.Opaque != "" {
		return false
	}
	if u.Scheme != "https" {
		ip := net.ParseIP(u.Hostname())
		if u.Scheme != "http" || !(u.Hostname() == "localhost" || ip != nil && ip.IsLoopback()) {
			return false
		}
	}
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return false
	}
	for _, key := range []string{"code", "state", "error", "error_description", "iss"} {
		if _, ok := query[key]; ok {
			return false
		}
	}
	return true
}

func normalizeOAuthClientRegistration(grants, redirects []string, method string) ([]string, []string, string, error) {
	fail := func() ([]string, []string, string, error) {
		return nil, nil, "", errors.New("invalid OAuth client registration")
	}
	if len(grants) == 0 {
		grants = []string{"client_credentials"}
	}
	grants = append([]string{}, grants...)
	for _, grant := range grants {
		if !slices.Contains([]string{"client_credentials", "authorization_code", "refresh_token"}, grant) {
			return fail()
		}
	}
	slices.Sort(grants)
	grants = slices.Compact(grants)
	code := slices.Contains(grants, "authorization_code")
	if slices.Contains(grants, "refresh_token") && !code {
		return fail()
	}
	if code && !slices.Contains(grants, "refresh_token") {
		grants = append(grants, "refresh_token")
		slices.Sort(grants)
	}
	if method == "" {
		method = "client_secret_basic"
	}
	if !slices.Contains([]string{"client_secret_basic", "client_secret_post", "none"}, method) {
		return fail()
	}
	if method == "none" && (!code || slices.Contains(grants, "client_credentials")) {
		return fail()
	}
	if code {
		if len(redirects) == 0 || len(redirects) > 16 {
			return fail()
		}
		redirects = append([]string{}, redirects...)
		for _, uri := range redirects {
			if !validOAuthRedirect(uri) {
				return fail()
			}
		}
		slices.Sort(redirects)
		redirects = slices.Compact(redirects)
	} else if len(redirects) > 0 {
		return fail()
	}
	if redirects == nil {
		redirects = []string{}
	}
	return grants, redirects, method, nil
}
