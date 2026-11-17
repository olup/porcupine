package porcupine

import (
	"slices"

	"github.com/olup/porcupine/internal/auth"
)

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

func (s *Service) interactiveOAuthEnabled() bool { return s.oauth != nil && s.oauth.browser != nil }

func (s *Service) interactiveResourceAllowed(resource string) bool {
	return resource == s.resources.mcpResource || s.oauth.config.Email != nil && resource == s.resources.apiResource
}

func (s *Service) interactiveScopesAllowed(human auth.Principal, client *oauthClient, resource string, permissions []string) bool {
	if len(permissions) == 0 || !s.interactiveResourceAllowed(resource) {
		return false
	}
	if !human.EmailHuman && resource != s.resources.mcpResource {
		return false
	}
	for _, permission := range permissions {
		if !slices.Contains(auth.Permissions(), permission) || !human.Can(permission) || !client.Principal.PermitsInteractiveScope(permission, client.GrantTypes) {
			return false
		}
		if !human.EmailHuman && !slices.Contains(auth.InteractiveReadScopes(), permission) {
			return false
		}
	}
	return true
}
