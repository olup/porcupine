package main

import (
	"errors"
	"os"
	"strconv"

	"github.com/olup/porcupine/internal/porcupine"
)

func oauthConfiguration() (*porcupine.OAuthConfig, error) {
	interactive, err := oidcConfiguration()
	if err != nil {
		return nil, err
	}
	email, err := emailConfiguration()
	if err != nil {
		return nil, err
	}
	issuer := os.Getenv("PORCUPINE_OAUTH_ISSUER")
	if issuer == "" {
		if os.Getenv("PORCUPINE_OAUTH_TOKEN_LIFETIME_SECONDS") != "" || os.Getenv("PORCUPINE_OAUTH_ALLOW_LEGACY_KEYS") != "" || os.Getenv("PORCUPINE_LOCAL_INVITATIONS") != "" {
			return nil, errors.New("PORCUPINE_OAUTH_ISSUER is required when OAuth is configured")
		}
		return nil, nil
	}
	config := &porcupine.OAuthConfig{Issuer: issuer, OIDC: interactive, Email: email}
	if raw := os.Getenv("PORCUPINE_LOCAL_INVITATIONS"); raw != "" {
		enabled, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, errors.New("invalid PORCUPINE_LOCAL_INVITATIONS")
		}
		config.LocalInvitations = enabled
	}
	if raw := os.Getenv("PORCUPINE_OAUTH_TOKEN_LIFETIME_SECONDS"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 60 || value > 3600 {
			return nil, errors.New("invalid PORCUPINE_OAUTH_TOKEN_LIFETIME_SECONDS")
		}
		config.AccessTokenLifetimeSeconds = value
	}
	if raw := os.Getenv("PORCUPINE_OAUTH_ALLOW_LEGACY_KEYS"); raw != "" {
		value, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, errors.New("invalid PORCUPINE_OAUTH_ALLOW_LEGACY_KEYS")
		}
		config.AllowLegacyKeys = value
	}
	return config, nil
}
