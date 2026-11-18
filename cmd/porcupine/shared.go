package main

import (
	"errors"
	"os"
	"strings"

	"github.com/olup/porcupine/internal/porcupine"
)

func (s settings) sharedConfiguration() (*porcupine.SharedConfig, error) {
	database, err := s.secretSetting("PORCUPINE_CONTROL_DATABASE_URL")
	if err != nil {
		return nil, err
	}
	if database == "" {
		return nil, nil
	}
	return &porcupine.SharedConfig{DatabaseURL: database}, nil
}

func (s settings) secretSetting(name string) (string, error) {
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
