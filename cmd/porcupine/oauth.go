package main

import (
	"errors"
	"os"
	"strconv"

	"github.com/olup/porcupine/internal/porcupine"
)

func oauthConfiguration() (*porcupine.OAuthConfig, error) {
	issuer := os.Getenv("PORCUPINE_OAUTH_ISSUER")
	if issuer == "" {
		if os.Getenv("PORCUPINE_OAUTH_TOKEN_LIFETIME_SECONDS") != "" || os.Getenv("PORCUPINE_OAUTH_ALLOW_LEGACY_KEYS") != "" {
			return nil, errors.New("PORCUPINE_OAUTH_ISSUER is required when OAuth is configured")
		}
		return nil, nil
	}
	config := &porcupine.OAuthConfig{Issuer: issuer}
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
