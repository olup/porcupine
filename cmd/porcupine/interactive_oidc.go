package main

import (
	"errors"
	"strings"

	"github.com/olup/porcupine/internal/porcupine"
)

func (s settings) oidcConfiguration() (*porcupine.OIDCConfig, error) {
	if s.get("PORCUPINE_OIDC_BINDINGS_FILE") != "" {
		return nil, errors.New("PORCUPINE_OIDC_BINDINGS_FILE is not supported; invite human accounts through the API")
	}
	issuer := s.get("PORCUPINE_OIDC_ISSUER")
	clientID := s.get("PORCUPINE_OIDC_CLIENT_ID")
	secret, err := s.secretSetting("PORCUPINE_OIDC_CLIENT_SECRET")
	if err != nil {
		return nil, err
	}
	if len(secret) > 16<<10 || strings.ContainsAny(secret, "\x00\r\n\t") {
		return nil, errors.New("invalid PORCUPINE_OIDC_CLIENT_SECRET")
	}
	if issuer == "" && clientID == "" && secret == "" {
		return nil, nil
	}
	if issuer == "" || clientID == "" || secret == "" || s.get("PORCUPINE_OAUTH_ISSUER") == "" {
		return nil, errors.New("OIDC requires PORCUPINE_OAUTH_ISSUER, PORCUPINE_OIDC_ISSUER, PORCUPINE_OIDC_CLIENT_ID and PORCUPINE_OIDC_CLIENT_SECRET or its _FILE option")
	}
	return &porcupine.OIDCConfig{Issuer: issuer, ClientID: clientID, ClientSecret: secret}, nil
}
