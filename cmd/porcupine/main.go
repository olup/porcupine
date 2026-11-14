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

func main() {
	if len(os.Args) == 2 && os.Args[1] == "--version" {
		fmt.Println(porcupine.Version)
		return
	}
	if len(os.Args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: porcupine [--version]")
		os.Exit(2)
	}
	addr := os.Getenv("PORCUPINE_ADDR")
	if addr == "" {
		addr = "127.0.0.1:8080"
	}
	queueMax := 100000
	if raw := os.Getenv("PORCUPINE_QUEUE_MAX_EVENTS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			slog.Error("PORCUPINE_QUEUE_MAX_EVENTS must be a positive integer")
			os.Exit(1)
		}
		queueMax = n
	}
	hotReceiptLimit := int64(0)
	if raw := os.Getenv("PORCUPINE_HOT_RECEIPT_LIMIT"); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n < 0 {
			slog.Error("PORCUPINE_HOT_RECEIPT_LIMIT must be a nonnegative integer")
			os.Exit(1)
		}
		hotReceiptLimit = n
	}
	queueMaxBytes, err := queueMaxBytesFromEnvironment()
	if err != nil {
		slog.Error("queue configuration", "error", err)
		os.Exit(1)
	}
	var allowedOrigins []string
	if raw := os.Getenv("PORCUPINE_ALLOWED_ORIGINS"); raw != "" {
		allowedOrigins = strings.Split(raw, ",")
	}
	var trustedProxies []string
	if raw := os.Getenv("PORCUPINE_TRUSTED_PROXIES"); raw != "" {
		trustedProxies = strings.Split(raw, ",")
	}
	clickHouse, duckLake, err := analyticsConfiguration()
	if err != nil {
		slog.Error("analytics configuration", "error", err)
		os.Exit(1)
	}
	exportConfig, err := exportConfiguration()
	if err != nil {
		slog.Error("export configuration", "error", err)
		os.Exit(1)
	}
	pipelineConfig, err := pipelineConfiguration()
	if err != nil {
		slog.Error("pipeline configuration", "error", err)
		os.Exit(1)
	}
	oauthConfig, err := oauthConfiguration()
	if err != nil {
		slog.Error("OAuth configuration", "error", err)
		os.Exit(1)
	}
	backupConfig, err := nativeBackupConfiguration()
	if err != nil {
		slog.Error("native backup configuration", "error", err)
		os.Exit(1)
	}
	s, err := porcupine.Open(porcupine.Config{DBPath: os.Getenv("PORCUPINE_DB"), BootstrapToken: os.Getenv("PORCUPINE_BOOTSTRAP_TOKEN"), QueueMaxEvents: queueMax, HotReceiptLimit: hotReceiptLimit, QueueMaxBytes: queueMaxBytes, AllowedOrigins: allowedOrigins, TrustedProxies: trustedProxies, ClickHouse: clickHouse, DuckLake: duckLake, Exports: exportConfig, Pipelines: pipelineConfig, OAuth: oauthConfig, NativeBackup: backupConfig})
	if err != nil {
		slog.Error("open storage", "error", err)
		os.Exit(1)
	}
	defer s.Close()
	server := &http.Server{Addr: addr, Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			slog.Error("shutdown", "error", err)
		}
	}()
	slog.Info("Porcupine listening", "address", addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("HTTP server", "error", err)
		os.Exit(1)
	}
}

func queueMaxBytesFromEnvironment() (int64, error) {
	raw := os.Getenv("PORCUPINE_QUEUE_MAX_BYTES")
	if raw == "" {
		return porcupine.DefaultQueueMaxBytes, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 {
		return 0, errors.New("PORCUPINE_QUEUE_MAX_BYTES must be a positive integer")
	}
	return value, nil
}
