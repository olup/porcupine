package auth

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"
)

// LoginProof binds a browser login to the account's current recovery version.
// Authorization must recheck this version when issuing a grant.
type LoginProof struct {
	Version  int64 `json:"version"`
	Verified bool  `json:"verified"`
}

type LoginIdentity struct {
	ID    string
	Proof LoginProof
}

// LoginAccounts keeps account authority and linking transactions in the control store.
type LoginAccounts interface {
	LookupEmail(context.Context, string) (LoginIdentity, error)
	ValidateLogin(context.Context, LoginIdentity) error
	LinkOIDCIdentity(context.Context, string, string) (LoginIdentity, error)
}

var ErrLoginRevoked = errors.New("login account access revoked")

// LoginSession contains the proof handed from sign-in to authorization consent.
// Provider verifier material and OTP challenges remain private to Login.
type LoginSession struct {
	Proof     LoginProof
	RequestID string
	CSRF      string
	HumanID   string
	Expires   time.Time
}

type LoginRequest struct{ ID, ClientName string }

type LoginConfig struct {
	Issuer   string
	OIDC     *OIDCConfig
	SendCode func(context.Context, string, string) error
}

// Login owns the browser sign-in protocol and its temporary, single-use state.
// Account lookup and proof verification are required for each login operation.
type Login struct {
	provider      *OIDCProvider
	issuer        string
	sendCode      func(context.Context, string, string) error
	sharedDB      *sql.DB
	emailDelivery EmailDeliveryLimiter
	mu            sync.Mutex
	sessions      map[string]*browserSession
	secure        bool
}

func NewLogin(ctx context.Context, config LoginConfig) (*Login, error) {
	issuer, err := NormalizeIssuer(config.Issuer, true)
	if err != nil {
		return nil, err
	}
	login := &Login{issuer: issuer, sendCode: config.SendCode,
		secure: strings.HasPrefix(issuer, "https://"), sessions: make(map[string]*browserSession)}
	if config.OIDC != nil {
		login.provider, err = NewOIDCProvider(ctx, *config.OIDC, issuer)
		if err != nil {
			return nil, err
		}
	}
	return login, nil
}

// ShareSessions selects the authoritative shared store before serving requests.
// The caller owns the database and its versioned schema.
func (o *Login) ShareSessions(db *sql.DB) { o.sharedDB = db }

func (o *Login) HasOIDC() bool { return o.provider != nil }

func (o *Login) ProviderIssuer() string {
	if o.provider == nil {
		return ""
	}
	return o.provider.Issuer()
}

func (o *Login) Start(w http.ResponseWriter, r *http.Request, request LoginRequest) {
	LoginPageHeaders(w)
	if o.sendCode != nil {
		o.startEmailLogin(w, request)
		return
	}
	o.startOIDCLogin(w, r, request.ID, time.Now().Add(LoginLifetime))
}
