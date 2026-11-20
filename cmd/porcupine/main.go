package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"log/slog"
	"net"
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
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	if err := run(os.Args[1:], os.Stdin, os.Stdout); err != nil {
		var failure *serverFailure
		if errors.As(err, &failure) {
			slog.Error("Porcupine server failed", "stage", failure.stage, "reason", serverFailureReason(failure.err))
		} else {
			fmt.Fprintln(os.Stderr, err)
		}
		os.Exit(1)
	}
}

type serverFailure struct {
	stage string
	err   error
}

func (e *serverFailure) Error() string { return "Porcupine server failed during " + e.stage }
func (e *serverFailure) Unwrap() error { return e.err }

func serverFailureReason(err error) string {
	switch {
	case errors.Is(err, syscall.EADDRINUSE):
		return "address_in_use"
	case errors.Is(err, syscall.EADDRNOTAVAIL):
		return "address_unavailable"
	case errors.Is(err, fs.ErrPermission):
		return "permission_denied"
	case errors.Is(err, fs.ErrNotExist):
		return "missing_file"
	case errors.Is(err, syscall.ENOSPC):
		return "disk_full"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, porcupine.ErrBootstrapTokenRequired):
		return "bootstrap_token_required"
	case errors.Is(err, porcupine.ErrRestoreInProgress), errors.Is(err, porcupine.ErrRestoreIncomplete):
		return "restore_incomplete"
	}
	return "unclassified"
}

type serverLog struct{ listener string }

func (l serverLog) Write(data []byte) (int, error) {
	// net/http may include peer information or arbitrary panic values.
	slog.Warn("HTTP server error", "listener", l.listener)
	return len(data), nil
}

func serve(config porcupine.Config, addr, metricsAddr string) (result error) {
	s, err := porcupine.Open(config)
	if err != nil {
		return &serverFailure{stage: "open_storage", err: err}
	}
	defer func() {
		if err := s.Close(); err != nil {
			result = errors.Join(result, &serverFailure{stage: "close_storage", err: err})
		}
	}()

	var private net.Listener
	if metricsAddr != "" {
		private, err = net.Listen("tcp", metricsAddr)
		if err != nil {
			return &serverFailure{stage: "bind_metrics", err: err}
		}
		defer private.Close()
	}
	public, err := net.Listen("tcp", addr)
	if err != nil {
		return &serverFailure{stage: "bind_api", err: err}
	}
	defer public.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	api := &http.Server{
		Handler: s.Handler(), ErrorLog: log.New(serverLog{"api"}, "", 0),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second,
		WriteTimeout: analyticsHTTPWriteTimeout(config.DuckLake) + time.Duration(config.InteractiveQueries.TimeoutMS)*time.Millisecond,
		IdleTimeout:  60 * time.Second, MaxHeaderBytes: 16 << 10,
	}
	exits := make(chan error, 2)
	go func() {
		exits <- &serverFailure{stage: "serve_api", err: api.Serve(public)}
	}()
	slog.Info("Porcupine listening", "listener", "api", "address", public.Addr().String())

	var metrics *http.Server
	if private != nil {
		metrics = &http.Server{
			Handler: s.MetricsHandler(), ErrorLog: log.New(serverLog{"metrics"}, "", 0),
			ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second,
			IdleTimeout: 30 * time.Second, MaxHeaderBytes: 4 << 10,
		}
		go func() {
			exits <- &serverFailure{stage: "serve_metrics", err: metrics.Serve(private)}
		}()
		slog.Info("Porcupine listening", "listener", "metrics", "address", private.Addr().String())
	}

	select {
	case <-ctx.Done():
	case err := <-exits:
		if !errors.Is(err, http.ErrServerClosed) {
			result = err
		}
	}
	s.BeginDrain()
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	servers := []*http.Server{api}
	if metrics != nil {
		servers = append(servers, metrics)
	}
	for _, server := range servers {
		if err := server.Shutdown(shutdown); err != nil {
			_ = server.Close()
			result = errors.Join(result, &serverFailure{stage: "shutdown_http", err: err})
		}
	}
	return result
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
	var queueProjectMax int
	if raw := s.get("PORCUPINE_QUEUE_PROJECT_MAX_EVENTS"); raw != "" {
		queueProjectMax, err = strconv.Atoi(raw)
		if err != nil || queueProjectMax < 0 {
			return "", porcupine.Config{}, errors.New("PORCUPINE_QUEUE_PROJECT_MAX_EVENTS must be a nonnegative integer")
		}
	}
	var queueProjectMaxBytes int64
	if raw := s.get("PORCUPINE_QUEUE_PROJECT_MAX_BYTES"); raw != "" {
		queueProjectMaxBytes, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || queueProjectMaxBytes < 0 {
			return "", porcupine.Config{}, errors.New("PORCUPINE_QUEUE_PROJECT_MAX_BYTES must be a nonnegative integer")
		}
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
	recoveryFloor, err := s.hostedRecoveryFloor()
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
	authConfig, err := s.authConfiguration()
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
	if s.get("PORCUPINE_PROFILE") == string(porcupine.ProfileHosted) && sharedConfig != nil && duckLake.PostgresURL == "" {
		duckLake.PostgresURL = sharedConfig.DatabaseURL
	}
	retention, err := s.receiptRetention()
	if err != nil {
		return "", porcupine.Config{}, err
	}
	usageConfig, err := s.usageConfiguration()
	if err != nil {
		return "", porcupine.Config{}, err
	}
	var flushInterval time.Duration
	if raw := s.get("PORCUPINE_ACCEPTANCE_FLUSH_INTERVAL"); raw != "" {
		flushInterval, err = time.ParseDuration(raw)
		if err != nil || flushInterval <= 0 {
			return "", porcupine.Config{}, errors.New("PORCUPINE_ACCEPTANCE_FLUSH_INTERVAL must be a positive duration")
		}
	}
	return addr, porcupine.Config{Profile: porcupine.DeploymentProfile(s.get("PORCUPINE_PROFILE")), AcceptanceMode: porcupine.AcceptanceMode(s.get("PORCUPINE_ACCEPTANCE_MODE")), HostedFlushInterval: flushInterval, Usage: usageConfig, ReceiptRetention: retention, DBPath: s.get("PORCUPINE_DB"), ControlDBPath: s.get("PORCUPINE_CONTROL_DB"), BootstrapToken: s.get("PORCUPINE_BOOTSTRAP_TOKEN"), QueueMaxEvents: queueMax, QueueMaxBytes: queueMaxBytes, QueueProjectMaxEvents: queueProjectMax, QueueProjectMaxBytes: queueProjectMaxBytes, AllowedOrigins: allowedOrigins, TrustedProxies: trustedProxies, GeoIPDatabase: s.get("PORCUPINE_GEOIP_DATABASE"), DuckLake: duckLake, HostedRecoveryFloor: recoveryFloor, Exports: exportConfig, Pipelines: pipelineConfig, OAuth: oauthConfig, Auth: authConfig, NativeBackup: backupConfig, InteractiveQueries: interactiveConfig, Shared: sharedConfig}, nil
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
