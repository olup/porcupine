package main

import (
	"errors"
	"strconv"

	"github.com/olup/porcupine/internal/porcupine"
)

func (s settings) oauthConfiguration() (*porcupine.OAuthConfig, error) {
	interactive, err := s.oidcConfiguration()
	if err != nil {
		return nil, err
	}
	email, err := s.emailConfiguration()
	if err != nil {
		return nil, err
	}
	issuer := s.get("PORCUPINE_OAUTH_ISSUER")
	if issuer == "" {
		if s.get("PORCUPINE_OAUTH_TOKEN_LIFETIME_SECONDS") != "" || s.get("PORCUPINE_OAUTH_ALLOW_LEGACY_KEYS") != "" || s.get("PORCUPINE_RESTORE_IDENTITY_BOOTSTRAP") != "" {
			return nil, errors.New("PORCUPINE_OAUTH_ISSUER is required when OAuth is configured")
		}
		return nil, nil
	}
	config := &porcupine.OAuthConfig{Issuer: issuer, OIDC: interactive, Email: email}
	if raw := s.get("PORCUPINE_OAUTH_TOKEN_LIFETIME_SECONDS"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 60 || value > 3600 {
			return nil, errors.New("invalid PORCUPINE_OAUTH_TOKEN_LIFETIME_SECONDS")
		}
		config.AccessTokenLifetimeSeconds = value
	}
	if raw := s.get("PORCUPINE_OAUTH_ALLOW_LEGACY_KEYS"); raw != "" {
		value, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, errors.New("invalid PORCUPINE_OAUTH_ALLOW_LEGACY_KEYS")
		}
		config.AllowLegacyKeys = value
	}
	if raw := s.get("PORCUPINE_RESTORE_IDENTITY_BOOTSTRAP"); raw != "" {
		value, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, errors.New("invalid PORCUPINE_RESTORE_IDENTITY_BOOTSTRAP")
		}
		config.AllowRestoredIdentityBootstrap = value
	}
	return config, nil
}
