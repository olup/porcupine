package porcupine

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"sync"
	"time"
)

const sharedBrowserTimeout = 3 * time.Second

// Browser state contains only temporary OAuth verifier material and OTP hashes.
// The cookie itself and the emailed code are never persisted.
type sharedBrowserSession struct {
	RequestID          string    `json:"request_id"`
	State              string    `json:"state,omitempty"`
	Nonce              string    `json:"nonce,omitempty"`
	Verifier           string    `json:"verifier,omitempty"`
	CSRF               string    `json:"csrf"`
	HumanID            string    `json:"human_id,omitempty"`
	Expires            time.Time `json:"expires"`
	ChallengeDigest    [32]byte  `json:"challenge_digest"`
	ChallengePrincipal string    `json:"challenge_principal,omitempty"`
	ChallengeAttempts  int       `json:"challenge_attempts"`
	ChallengeExpires   time.Time `json:"challenge_expires"`
}

func encodeBrowserSession(s *oidcBrowserSession) ([]byte, error) {
	return json.Marshal(sharedBrowserSession{RequestID: s.requestID, State: s.state, Nonce: s.nonce, Verifier: s.verifier, CSRF: s.csrf, HumanID: s.humanID, Expires: s.expires,
		ChallengeDigest: s.emailChallenge.digest, ChallengePrincipal: s.emailChallenge.principal, ChallengeAttempts: s.emailChallenge.attemptsLeft, ChallengeExpires: s.emailChallenge.expires})
}

func decodeBrowserSession(raw string) (*oidcBrowserSession, error) {
	var s sharedBrowserSession
	if err := json.Unmarshal([]byte(raw), &s); err != nil {
		return nil, err
	}
	return &oidcBrowserSession{requestID: s.RequestID, state: s.State, nonce: s.Nonce, verifier: s.Verifier, csrf: s.CSRF, humanID: s.HumanID, expires: s.Expires,
		emailChallenge: emailChallenge{digest: s.ChallengeDigest, principal: s.ChallengePrincipal, attemptsLeft: s.ChallengeAttempts, expires: s.ChallengeExpires}}, nil
}

func (s *Service) initializeSharedBrowser(ctx context.Context) error {
	if s.shared == nil || s.oauth == nil || s.oauth.browser == nil {
		return nil
	}
	_, err := s.control.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS oauth_browser_sessions (
 session_hash TEXT PRIMARY KEY,payload TEXT NOT NULL,expires_at_ms BIGINT NOT NULL);
CREATE INDEX IF NOT EXISTS oauth_browser_sessions_expiry ON oauth_browser_sessions(expires_at_ms);
CREATE TABLE IF NOT EXISTS oauth_email_delivery_counts (
 period BIGINT NOT NULL,scope TEXT NOT NULL,count BIGINT NOT NULL,PRIMARY KEY(period,scope));
CREATE TABLE IF NOT EXISTS oauth_email_delivery_attempts (
 id TEXT PRIMARY KEY,expires_at_ms BIGINT NOT NULL);
CREATE INDEX IF NOT EXISTS oauth_email_attempts_expiry ON oauth_email_delivery_attempts(expires_at_ms);`)
	if err == nil {
		s.oauth.browser.sharedDB = s.control
	}
	return err
}

func (o *oauthBrowser) saveSharedSession(session *oidcBrowserSession) (string, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), sharedBrowserTimeout)
	defer cancel()
	now := time.Now()
	if !now.Before(session.expires) {
		return "", false
	}
	raw, err := encodeBrowserSession(session)
	if err != nil {
		return "", false
	}
	tx, err := o.sharedDB.BeginTx(ctx, nil)
	if err != nil {
		return "", false
	}
	defer tx.Rollback()
	// Capacity is one deployment budget, rather than one allowance per replica.
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(2081294131)`); err != nil {
		return "", false
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM oauth_browser_sessions WHERE expires_at_ms<=$1`, now.UnixMilli()); err != nil {
		return "", false
	}
	var count int
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM oauth_browser_sessions`).Scan(&count); err != nil || count >= oidcSessionCapacity {
		return "", false
	}
	cookie := oidcRandom()
	if _, err = tx.ExecContext(ctx, `INSERT INTO oauth_browser_sessions(session_hash,payload,expires_at_ms) VALUES($1,$2,$3)`, hash(cookie), string(raw), session.expires.UnixMilli()); err != nil {
		return "", false
	}
	return cookie, tx.Commit() == nil
}

func (o *oauthBrowser) loadSharedSession(parent context.Context, key string) *oidcBrowserSession {
	ctx, cancel := context.WithTimeout(parent, sharedBrowserTimeout)
	defer cancel()
	var raw string
	err := o.sharedDB.QueryRowContext(ctx, `SELECT payload FROM oauth_browser_sessions WHERE session_hash=$1 AND expires_at_ms>$2`, key, time.Now().UnixMilli()).Scan(&raw)
	if err != nil {
		return nil
	}
	session, err := decodeBrowserSession(raw)
	if err != nil {
		return nil
	}
	return session
}

func (o *oauthBrowser) consumeSharedSession(key string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), sharedBrowserTimeout)
	defer cancel()
	result, err := o.sharedDB.ExecContext(ctx, `DELETE FROM oauth_browser_sessions WHERE session_hash=$1 AND expires_at_ms>$2`, key, time.Now().UnixMilli())
	if err != nil {
		return false
	}
	count, err := result.RowsAffected()
	return err == nil && count == 1
}

func (o *oauthBrowser) redeemSharedEmailCode(key, code string, now time.Time) (*oidcBrowserSession, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), sharedBrowserTimeout)
	defer cancel()
	tx, err := o.sharedDB.BeginTx(ctx, nil)
	if err != nil {
		return nil, false
	}
	defer tx.Rollback()
	var raw string
	if err = tx.QueryRowContext(ctx, `SELECT payload FROM oauth_browser_sessions WHERE session_hash=$1 FOR UPDATE`, key).Scan(&raw); err != nil {
		return nil, false
	}
	session, err := decodeBrowserSession(raw)
	if err != nil {
		return nil, false
	}
	challenge := &session.emailChallenge
	if challenge.expires.IsZero() {
		return nil, false
	}
	if !now.Before(challenge.expires) || !now.Before(session.expires) {
		if _, err := tx.ExecContext(ctx, `DELETE FROM oauth_browser_sessions WHERE session_hash=$1`, key); err == nil {
			_ = tx.Commit()
		}
		return nil, false
	}
	sum := sha256.Sum256([]byte(session.csrf + code))
	if subtle.ConstantTimeCompare(sum[:], challenge.digest[:]) != 1 || challenge.principal == "" {
		challenge.attemptsLeft--
		if challenge.attemptsLeft <= 0 {
			_, err = tx.ExecContext(ctx, `DELETE FROM oauth_browser_sessions WHERE session_hash=$1`, key)
		} else {
			var encoded []byte
			encoded, err = encodeBrowserSession(session)
			if err == nil {
				_, err = tx.ExecContext(ctx, `UPDATE oauth_browser_sessions SET payload=$1 WHERE session_hash=$2`, string(encoded), key)
			}
		}
		if err == nil {
			_ = tx.Commit()
		}
		return nil, false
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM oauth_browser_sessions WHERE session_hash=$1`, key); err != nil {
		return nil, false
	}
	if tx.Commit() != nil {
		return nil, false
	}
	return &oidcBrowserSession{requestID: session.requestID, expires: session.expires, humanID: challenge.principal, csrf: oidcRandom()}, true
}

func (o *oauthBrowser) reserveEmailAttempt(now time.Time, address, peer string) (func(), bool) {
	if o.sharedDB == nil {
		if !o.reserveEmailDelivery(now, address, peer) {
			return nil, false
		}
		var once sync.Once
		return func() { once.Do(o.finishEmailDelivery) }, true
	}
	ctx, cancel := context.WithTimeout(context.Background(), sharedBrowserTimeout)
	defer cancel()
	tx, err := o.sharedDB.BeginTx(ctx, nil)
	if err != nil {
		return nil, false
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(2081294132)`); err != nil {
		return nil, false
	}
	period := now.UnixMilli() / emailDeliveryPeriod.Milliseconds()
	if _, err = tx.ExecContext(ctx, `DELETE FROM oauth_email_delivery_counts WHERE period<$1`, period); err != nil {
		return nil, false
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM oauth_email_delivery_attempts WHERE expires_at_ms<=$1`, now.UnixMilli()); err != nil {
		return nil, false
	}
	var inFlight int
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM oauth_email_delivery_attempts`).Scan(&inFlight); err != nil || inFlight >= 8 {
		return nil, false
	}
	for _, budget := range []struct {
		scope string
		limit int
	}{{"instance", 120}, {"address:" + hash(address), 3}, {"peer:" + peer, 20}} {
		var used int
		err = tx.QueryRowContext(ctx, `SELECT count FROM oauth_email_delivery_counts WHERE period=$1 AND scope=$2`, period, budget.scope).Scan(&used)
		if err != nil && err != sql.ErrNoRows || used >= budget.limit {
			return nil, false
		}
	}
	for _, scope := range []string{"instance", "address:" + hash(address), "peer:" + peer} {
		if _, err = tx.ExecContext(ctx, `INSERT INTO oauth_email_delivery_counts(period,scope,count) VALUES($1,$2,1) ON CONFLICT(period,scope) DO UPDATE SET count=oauth_email_delivery_counts.count+1`, period, scope); err != nil {
			return nil, false
		}
	}
	id := oidcRandom()
	// Sending itself has a 15-second deadline. Expiry recovers a crashed sender.
	if _, err = tx.ExecContext(ctx, `INSERT INTO oauth_email_delivery_attempts(id,expires_at_ms) VALUES($1,$2)`, id, now.Add(20*time.Second).UnixMilli()); err != nil {
		return nil, false
	}
	if tx.Commit() != nil {
		return nil, false
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			ctx, cancel := context.WithTimeout(context.Background(), sharedBrowserTimeout)
			defer cancel()
			_, _ = o.sharedDB.ExecContext(ctx, `DELETE FROM oauth_email_delivery_attempts WHERE id=$1`, id)
		})
	}, true
}
