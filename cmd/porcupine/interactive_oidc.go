package main

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"

	"github.com/olup/porcupine/internal/porcupine"
)

func oidcConfiguration() (*porcupine.OIDCConfig, error) {
	issuer := os.Getenv("PORCUPINE_OIDC_ISSUER")
	clientID := os.Getenv("PORCUPINE_OIDC_CLIENT_ID")
	secretFile := os.Getenv("PORCUPINE_OIDC_CLIENT_SECRET_FILE")
	bindingsFile := os.Getenv("PORCUPINE_OIDC_BINDINGS_FILE")
	if issuer == "" && clientID == "" && secretFile == "" && bindingsFile == "" {
		return nil, nil
	}
	if issuer == "" || clientID == "" || secretFile == "" || bindingsFile == "" || os.Getenv("PORCUPINE_OAUTH_ISSUER") == "" {
		return nil, errors.New("OIDC requires PORCUPINE_OAUTH_ISSUER, PORCUPINE_OIDC_ISSUER, PORCUPINE_OIDC_CLIENT_ID, PORCUPINE_OIDC_CLIENT_SECRET_FILE and PORCUPINE_OIDC_BINDINGS_FILE")
	}
	read := func(path string, limit int64) ([]byte, error) {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		data, err := io.ReadAll(io.LimitReader(f, limit+1))
		if int64(len(data)) > limit {
			return nil, errors.New("file too large")
		}
		return data, err
	}
	secret, err := read(secretFile, 16<<10)
	if err != nil {
		return nil, errors.New("cannot read bounded OIDC client secret file")
	}
	secretValue := strings.TrimRight(string(secret), "\r\n")
	if secretValue == "" {
		return nil, errors.New("empty OIDC client secret")
	}
	data, err := read(bindingsFile, 1<<20)
	if err != nil {
		return nil, errors.New("cannot read bounded OIDC bindings file")
	}
	var bindings []porcupine.OIDCBinding
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&bindings) != nil || decoder.Decode(new(any)) != io.EOF {
		return nil, errors.New("invalid OIDC bindings JSON")
	}
	return &porcupine.OIDCConfig{Issuer: issuer, ClientID: clientID, ClientSecret: secretValue, Bindings: bindings}, nil
}
