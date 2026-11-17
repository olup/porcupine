package porcupine

import (
	"context"
	"database/sql"
	"net/url"
	"sync"

	"github.com/olup/porcupine/internal/auth"
)

// OIDCConfig selects the trusted upstream provider; clients cannot override it.
type OIDCConfig struct{ Issuer, ClientID, ClientSecret string }

type interactiveOIDC struct {
	provider *auth.OIDCProvider
	*oauthBrowser
}

func newOAuthBrowser(secure bool) *oauthBrowser {
	return &oauthBrowser{secure: secure, sessions: map[string]*oidcBrowserSession{}}
}

type oauthBrowser struct {
	sharedDB      *sql.DB
	emailDelivery auth.EmailDeliveryLimiter
	mu            sync.Mutex
	sessions      map[string]*oidcBrowserSession
	secure        bool
}

func newInteractiveOIDC(config *OIDCConfig, issuer string) (*interactiveOIDC, error) {
	if config == nil {
		return nil, nil
	}
	provider, err := auth.NewOIDCProvider(context.Background(), auth.OIDCConfig{Issuer: config.Issuer, ClientID: config.ClientID, ClientSecret: config.ClientSecret}, issuer)
	if err != nil {
		return nil, err
	}
	origin, _ := url.Parse(issuer)
	return &interactiveOIDC{provider: provider, oauthBrowser: newOAuthBrowser(origin.Scheme == "https")}, nil
}
