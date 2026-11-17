package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/olup/porcupine/internal/porcupine"
)

func run(args []string, input io.Reader, output io.Writer) error {
	if len(args) == 1 && args[0] == "--version" {
		_, err := fmt.Fprintln(output, porcupine.Version)
		return err
	}
	path, explicit := os.LookupEnv("PORCUPINE_CONFIG")
	if path == "" {
		path, explicit = "porcupine.json", false
	}
	var command []string
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--config":
			i++
			if i == len(args) || args[i] == "" {
				return errors.New("--config requires a file path")
			}
			path, explicit = args[i], true
		case strings.HasPrefix(args[i], "--config="):
			path, explicit = strings.TrimPrefix(args[i], "--config="), true
			if path == "" {
				return errors.New("--config requires a file path")
			}
		default:
			command = append(command, args[i])
		}
	}
	if len(command) >= 1 && command[0] == "init" {
		docker := len(command) == 2 && command[1] == "--docker"
		if len(command) > 1 && !docker {
			return errors.New("usage: porcupine init [--config FILE] [--docker]")
		}
		if err := writeInitialConfig(path, docker); err != nil {
			return err
		}
		_, err := fmt.Fprintf(output, "Created %s with private permissions. Keep it out of source control.\nRun porcupine config check --config %s before starting.\n", path, path)
		return err
	}
	if len(command) == 2 && command[0] == "config" && command[1] == "install" {
		if err := installConfig(path, input); err != nil {
			return err
		}
		_, err := fmt.Fprintln(output, "Installed private configuration. Restart the server to apply it.")
		return err
	}
	check := len(command) == 2 && command[0] == "config" && command[1] == "check"
	restore := len(command) == 1 && command[0] == "restore-native-backup"
	if len(command) != 0 && !check && !restore {
		return errors.New("usage: porcupine [--config FILE] [--version|init|config check|config install|restore-native-backup]")
	}
	s, err := loadSettings(path, explicit)
	if err != nil {
		return err
	}
	if restore {
		return s.runNativeRestore(context.Background(), input, output)
	}
	addr, config, err := s.serviceConfig()
	if err != nil {
		return err
	}
	if err := validateServerConfig(addr, config); err != nil {
		return err
	}
	if check {
		values := s.redacted()
		defaults := map[string]string{
			"PORCUPINE_ADDR": addr, "PORCUPINE_DB": "./data/porcupine.db",
			"PORCUPINE_QUEUE_MAX_EVENTS": "100000", "PORCUPINE_QUEUE_MAX_BYTES": strconv.FormatInt(porcupine.DefaultQueueMaxBytes, 10),
			"PORCUPINE_QUERY_MAX_CONCURRENT": "2", "PORCUPINE_QUERY_TIMEOUT_MS": "5000", "PORCUPINE_QUERY_MAX_ROWS": "1000", "PORCUPINE_QUERY_MAX_RESULT_BYTES": "1048576",
			"PORCUPINE_DUCKLAKE_DIR": config.DuckLake.Directory,
		}
		for key, value := range defaults {
			if _, ok := values[key]; !ok {
				values[key] = value
			}
		}
		if _, ok := values["PORCUPINE_CONTROL_DB"]; !ok && config.Shared == nil {
			values["PORCUPINE_CONTROL_DB"] = values["PORCUPINE_DB"] + ".control"
		}
		encoder := json.NewEncoder(output)
		encoder.SetIndent("", "  ")
		return encoder.Encode(values)
	}
	return serve(config, addr)
}

func validateServerConfig(addr string, config porcupine.Config) error {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return errors.New("listen must be a host:port address")
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 0 || n > 65535 {
		return errors.New("listen port must be between 0 and 65535")
	}
	if config.BootstrapToken != "" && len(config.BootstrapToken) < 16 {
		return errors.New("a configured bootstrap_token must have at least 16 characters; run porcupine init for a fresh instance")
	}
	if config.Shared != nil && (config.DuckLake.PostgresURL == "" || config.DuckLake.S3 == nil) {
		return errors.New("shared deployment requires PostgreSQL DuckLake metadata and analytical S3 data")
	}
	return porcupine.ValidateConfig(config)
}

func writeInitialConfig(path string, docker bool) error {
	var config FileConfig
	config.Listen = "127.0.0.1:8080"
	config.Storage.Journal, config.Storage.Control, config.Storage.DuckLake = "./data/porcupine.db", "./data/porcupine.db.control", "./data/ducklake"
	if docker {
		config.Listen = "0.0.0.0:8080"
		config.Storage.Journal, config.Storage.Control, config.Storage.DuckLake = "/data/porcupine.db", "/data/porcupine.db.control", "/data/ducklake"
	}
	var token, key [32]byte
	if _, err := rand.Read(token[:]); err != nil {
		return err
	}
	if _, err := rand.Read(key[:]); err != nil {
		return err
	}
	config.BootstrapToken = base64.RawURLEncoding.EncodeToString(token[:])
	config.Pipeline.Key = base64.StdEncoding.EncodeToString(key[:])
	content, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return errors.New("cannot create configuration directory")
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if errors.Is(err, os.ErrExist) {
		return errors.New("configuration file already exists; refusing to overwrite secrets")
	}
	if err != nil {
		return errors.New("cannot create private configuration file")
	}
	defer f.Close()
	if _, err := f.Write(append(content, '\n')); err != nil {
		return errors.New("cannot write private configuration file")
	}
	if err := f.Sync(); err != nil {
		return errors.New("cannot sync private configuration file")
	}
	return nil
}

func installConfig(path string, input io.Reader) error {
	if input == nil {
		return errors.New("config install requires a JSON object on standard input")
	}
	if err := validateEnvironment(); err != nil {
		return err
	}
	body, err := io.ReadAll(io.LimitReader(input, (1<<20)+1))
	if err != nil || len(body) > 1<<20 {
		return errors.New("configuration file must be at most 1 MiB")
	}
	s, err := readSettings(bytes.NewReader(body))
	if err != nil {
		return err
	}
	addr, config, err := s.serviceConfig()
	if err != nil {
		return err
	}
	if err := validateServerConfig(addr, config); err != nil {
		return err
	}
	directory := filepath.Dir(path)
	f, err := os.CreateTemp(directory, ".porcupine-config-*")
	if err != nil {
		return errors.New("cannot create private configuration replacement")
	}
	temp := f.Name()
	defer os.Remove(temp)
	defer f.Close()
	if _, err := f.Write(body); err != nil {
		return errors.New("cannot write private configuration replacement")
	}
	if err := f.Sync(); err != nil {
		return errors.New("cannot sync private configuration replacement")
	}
	if err := f.Close(); err != nil {
		return errors.New("cannot close private configuration replacement")
	}
	if err := os.Rename(temp, path); err != nil {
		return errors.New("cannot replace configuration file")
	}
	dir, err := os.Open(directory)
	if err != nil {
		return errors.New("cannot sync configuration directory; replacement was installed")
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return errors.New("cannot sync configuration directory; replacement was installed")
	}
	return nil
}
