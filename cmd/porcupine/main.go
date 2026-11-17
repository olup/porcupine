package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/olup/porcupine/internal/porcupine"
)

// Container builds set this at link time; explicit settings always take priority.
var defaultListenAddress = "127.0.0.1:8080"

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func serve(config porcupine.Config, addr string) error {
	s, err := porcupine.Open(config)
	if err != nil {
		return err
	}
	defer s.Close()
	server := &http.Server{Addr: addr, Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: analyticsHTTPWriteTimeout(config.DuckLake) + time.Duration(config.InteractiveQueries.TimeoutMS)*time.Millisecond, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		<-ctx.Done()
		s.BeginQueryDrain()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			slog.Error("shutdown", "error", err)
		}
	}()
	slog.Info("Porcupine listening", "address", addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	<-drained
	return nil
}

func (s settings) serviceConfig() (string, porcupine.Config, error) {
	addr := s.get("PORCUPINE_ADDR")
	if addr == "" {
		addr = defaultListenAddress
	}
	queueMax := 100000
	if raw := s.get("PORCUPINE_QUEUE_MAX_EVENTS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			return "", porcupine.Config{}, errors.New("PORCUPINE_QUEUE_MAX_EVENTS must be a positive integer")
		}
		queueMax = n
	}
	queueMaxBytes, err := s.queueMaxBytesFromEnvironment()
	if err != nil {
		return "", porcupine.Config{}, err
	}
	var allowedOrigins []string
	if raw := s.get("PORCUPINE_ALLOWED_ORIGINS"); raw != "" {
		allowedOrigins = strings.Split(raw, ",")
	}
	var trustedProxies []string
	if raw := s.get("PORCUPINE_TRUSTED_PROXIES"); raw != "" {
		trustedProxies = strings.Split(raw, ",")
	}
	duckLake, err := s.analyticsConfiguration()
	if err != nil {
		return "", porcupine.Config{}, err
	}
	interactiveConfig, err := s.interactiveQueryConfiguration()
	if err != nil {
		return "", porcupine.Config{}, err
	}
	exportConfig, err := s.exportConfiguration()
	if err != nil {
		return "", porcupine.Config{}, err
	}
	pipelineConfig, err := s.pipelineConfiguration()
	if err != nil {
		return "", porcupine.Config{}, err
	}
	oauthConfig, err := s.oauthConfiguration()
	if err != nil {
		return "", porcupine.Config{}, err
	}
	backupConfig, err := s.nativeBackupConfiguration()
	if err != nil {
		return "", porcupine.Config{}, err
	}
	sharedConfig, err := s.sharedConfiguration()
	if err != nil {
		return "", porcupine.Config{}, err
	}
	retention, err := s.receiptRetention()
	if err != nil {
		return "", porcupine.Config{}, err
	}
	return addr, porcupine.Config{AcceptanceMode: porcupine.AcceptanceMode(s.get("PORCUPINE_ACCEPTANCE_MODE")), ReceiptRetention: retention, DBPath: s.get("PORCUPINE_DB"), ControlDBPath: s.get("PORCUPINE_CONTROL_DB"), BootstrapToken: s.get("PORCUPINE_BOOTSTRAP_TOKEN"), QueueMaxEvents: queueMax, QueueMaxBytes: queueMaxBytes, AllowedOrigins: allowedOrigins, TrustedProxies: trustedProxies, DuckLake: duckLake, Exports: exportConfig, Pipelines: pipelineConfig, OAuth: oauthConfig, NativeBackup: backupConfig, InteractiveQueries: interactiveConfig, Shared: sharedConfig}, nil
}

func (s settings) receiptRetention() (time.Duration, error) {
	raw := s.get("PORCUPINE_RECEIPT_RETENTION")
	if raw == "" || raw == "0" {
		return 0, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value < 24*time.Hour {
		return 0, errors.New("PORCUPINE_RECEIPT_RETENTION must be 0 or a duration of at least 24h")
	}
	return value, nil
}

func (s settings) queueMaxBytesFromEnvironment() (int64, error) {
	raw := s.get("PORCUPINE_QUEUE_MAX_BYTES")
	if raw == "" {
		return porcupine.DefaultQueueMaxBytes, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 {
		return 0, errors.New("PORCUPINE_QUEUE_MAX_BYTES must be a positive integer")
	}
	return value, nil
}
