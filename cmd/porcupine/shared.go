package main

import (
	"errors"
	"os"
	"strings"

	"github.com/olup/porcupine/internal/porcupine"
)

func (s settings) sharedConfiguration() (*porcupine.SharedConfig, error) {
	database, err := s.sharedSecretSetting("PORCUPINE_CONTROL_DATABASE_URL")
	if err != nil {
		return nil, err
	}
	worker, route := s.get("PORCUPINE_WORKER_ID"), s.get("PORCUPINE_WORKER_URL")
	secret, err := s.sharedSecretSetting("PORCUPINE_SHARED_ROUTING_SECRET")
	if err != nil {
		return nil, err
	}
	if s.get("PORCUPINE_PROFILE") == string(porcupine.ProfileHosted) {
		return &porcupine.SharedConfig{DatabaseURL: database, WorkerID: worker, RouteURL: route, RoutingSecret: []byte(secret)}, nil
	}
	if database == "" && worker == "" && route == "" && secret == "" {
		return nil, nil
	}
	if database == "" || worker == "" || route == "" || len(secret) < 32 {
		return nil, errors.New("shared deployment requires PORCUPINE_CONTROL_DATABASE_URL, PORCUPINE_WORKER_ID, PORCUPINE_WORKER_URL and a routing secret of at least 32 bytes")
	}
	return &porcupine.SharedConfig{DatabaseURL: database, WorkerID: worker, RouteURL: route, RoutingSecret: []byte(secret)}, nil
}

func (s settings) sharedSecretSetting(name string) (string, error) {
	value, file := s.get(name), s.get(name+"_FILE")
	if value != "" && file != "" {
		return "", errors.New(name + " and its _FILE option are mutually exclusive")
	}
	if file != "" {
		info, err := os.Stat(file)
		if err != nil || !info.Mode().IsRegular() || info.Size() > 16<<10 {
			return "", errors.New("cannot read " + name + "_FILE")
		}
		raw, err := os.ReadFile(file)
		if err != nil {
			return "", errors.New("cannot read " + name + "_FILE")
		}
		value = strings.TrimSpace(string(raw))
		if value == "" {
			return "", errors.New("empty " + name + "_FILE")
		}
	}
	if strings.ContainsAny(value, "\x00\r\n") {
		return "", errors.New("invalid " + name)
	}
	return value, nil
}
