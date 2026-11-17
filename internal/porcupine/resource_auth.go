package porcupine

import "github.com/olup/porcupine/internal/auth"

// AuthConfig selects either the integrated identity provider or one trusted
// external access-token issuer. Native ingestion and machine keys work in both.
type AuthConfig struct {
	Mode      string
	PublicURL string
	External  auth.ExternalConfig
}

type resourceRuntime struct {
	publicURL   string
	apiResource string
	mcpResource string
	external    *auth.ExternalVerifier
}

func newResourceRuntime(publicURL string) *resourceRuntime {
	return &resourceRuntime{publicURL: publicURL, apiResource: publicURL + "/api/v1", mcpResource: publicURL + "/mcp"}
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
	resources := newResourceRuntime(public)
	resources.external = verifier
	return resources, nil
}
func (s *Service) externalAuthEnabled() bool {
	return s.resources != nil && s.resources.external != nil
}
