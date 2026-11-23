package main

import (
	"errors"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/olup/porcupine/internal/auth"
	"github.com/olup/porcupine/internal/porcupine"
)

func (s settings) oauthConfiguration() (*porcupine.OAuthConfig, error) {
	if s.get("PORCUPINE_AUTH_MODE") == "external" {
		var err error
		configFields(reflect.ValueOf(FileConfig{}), "", func(name string, _ reflect.Value, _ bool) {
			if s.get(name) != "" && (strings.HasPrefix(name, "PORCUPINE_OAUTH_") || strings.HasPrefix(name, "PORCUPINE_OIDC_") || strings.HasPrefix(name, "PORCUPINE_SMTP_") || name == "PORCUPINE_ADMIN_EMAIL" || name == "PORCUPINE_BOOTSTRAP_TOKEN" || name == "PORCUPINE_RESTORE_IDENTITY_BOOTSTRAP") {
				err = errors.New("external authentication cannot configure builtin OAuth, email, OIDC or bootstrap settings")
			}
		})
		return nil, err
	}
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
		if s.get("PORCUPINE_OAUTH_TOKEN_LIFETIME_SECONDS") != "" || s.get("PORCUPINE_RESTORE_IDENTITY_BOOTSTRAP") != "" {
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
	if raw := s.get("PORCUPINE_RESTORE_IDENTITY_BOOTSTRAP"); raw != "" {
		value, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, errors.New("invalid PORCUPINE_RESTORE_IDENTITY_BOOTSTRAP")
		}
		config.AllowRestoredIdentityBootstrap = value
	}
	return config, nil
}

func (s settings) authConfiguration() (*porcupine.AuthConfig, error) {
	mode := s.get("PORCUPINE_AUTH_MODE")
	if mode == "" {
		mode = "builtin"
	}
	if mode != "builtin" && mode != "external" {
		return nil, errors.New("PORCUPINE_AUTH_MODE must be builtin or external")
	}
	var externalSettings bool
	configFields(reflect.ValueOf(FileConfig{}), "", func(name string, _ reflect.Value, _ bool) {
		if strings.HasPrefix(name, "PORCUPINE_AUTH_EXTERNAL_") && s.get(name) != "" {
			externalSettings = true
		}
	})
	if mode == "builtin" {
		if externalSettings || s.get("PORCUPINE_AUTH_PUBLIC_URL") != "" {
			return nil, errors.New("external authentication settings require auth.mode external")
		}
		return nil, nil
	}
	c := &porcupine.AuthConfig{Mode: mode, PublicURL: s.get("PORCUPINE_AUTH_PUBLIC_URL")}
	c.External = auth.ExternalConfig{Issuer: s.get("PORCUPINE_AUTH_EXTERNAL_ISSUER"), JWKSURL: s.get("PORCUPINE_AUTH_EXTERNAL_JWKS_URL"), APIAudience: s.get("PORCUPINE_AUTH_EXTERNAL_API_AUDIENCE"), MCPAudience: s.get("PORCUPINE_AUTH_EXTERNAL_MCP_AUDIENCE")}
	if c.External.APIAudience == "" {
		c.External.APIAudience = c.PublicURL + "/api/v1"
	}
	if c.External.MCPAudience == "" {
		c.External.MCPAudience = c.PublicURL + "/mcp"
	}
	if raw := s.get("PORCUPINE_AUTH_EXTERNAL_ALGORITHMS"); raw != "" {
		c.External.Algorithms = strings.Split(raw, ",")
	}
	if raw := s.get("PORCUPINE_AUTH_EXTERNAL_TOKEN_TYPES"); raw != "" {
		c.External.TokenTypes = strings.Split(raw, ",")
	}
	for _, entry := range []struct {
		name   string
		target *time.Duration
	}{{"PORCUPINE_AUTH_EXTERNAL_CLOCK_SKEW", &c.External.ClockSkew}, {"PORCUPINE_AUTH_EXTERNAL_MAX_TOKEN_LIFETIME", &c.External.MaxTokenLifetime}} {
		if raw := s.get(entry.name); raw != "" {
			value, err := time.ParseDuration(raw)
			if err != nil {
				return nil, errors.New("invalid " + entry.name)
			}
			*entry.target = value
		}
	}
	return c, nil
}
