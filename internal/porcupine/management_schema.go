package porcupine

import (
	"net/http"

	"github.com/danielgtaylor/huma/v2"
)

// ManagementOpenAPI returns the management contract for offline tooling.
// Register routes on a private mux with the optional OAuth and email routes
// enabled. Building the schema never dispatches a request to a handler.
func ManagementOpenAPI() *huma.OpenAPI {
	s := Service{oauth: &oauthRuntime{
		issuer: "https://porcupine.example",
		config: OAuthConfig{Email: &EmailConfig{}},
	}}
	return s.registerManagement(http.NewServeMux()).OpenAPI()
}
