// shared-postgres-integration runs the shared-state suite against an isolated,
// disposable native PostgreSQL process bound only to loopback.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	postgres "github.com/fergusstrange/embedded-postgres/v2"
)

const postgresVersion postgres.PostgresVersion = "17.11.1"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pattern := flag.String("run", "^TestShared", "Go test name pattern")
	race := flag.Bool("race", false, "Enable the Go race detector")
	flag.Parse()
	command := flag.Args()
	if len(command) == 0 {
		command = []string{"go", "test", "./internal/porcupine", "./internal/lake", "./internal/auth", "-run", *pattern, "-count=1", "-timeout=600s"}
		if *race {
			command = append(command, "-race")
		}
	}
	if err := run(ctx, command); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, command []string) (result error) {
	password, err := randomHex(24)
	if err != nil {
		return err
	}
	cache := os.Getenv("PORCUPINE_TEST_POSTGRES_CACHE")
	if cache == "" {
		base, err := os.UserCacheDir()
		if err != nil {
			return fmt.Errorf("locate PostgreSQL binary cache: %w", err)
		}
		cache = filepath.Join(base, "porcupine-test-postgres")
	}
	pg := postgres.NewDatabase(postgres.DefaultConfig().Version(postgresVersion).
		Port(0).Database("porcupine_integration").Password(password).
		Storage(postgres.Storage{CacheDir: cache}).
		StartParameters(map[string]string{
			"shared_buffers": "32MB", "max_connections": "100",
			"fsync": "on", "synchronous_commit": "on", "full_page_writes": "on",
		}))
	defer func() {
		result = errors.Join(result, pg.Close())
		if result != nil {
			fmt.Fprint(os.Stderr, pg.Logs())
		}
	}()
	if err := pg.StartContext(ctx); err != nil {
		return fmt.Errorf("start isolated native PostgreSQL fixture: %w", err)
	}
	fmt.Fprintln(os.Stderr, "PostgreSQL 17 fixture ready (native process, loopback only)")
	child := exec.CommandContext(ctx, command[0], command[1:]...)
	child.Env = environment(map[string]string{"PORCUPINE_TEST_POSTGRES_DSN": pg.ConnectionURL(), "CGO_ENABLED": "1"})
	child.Stdin, child.Stdout, child.Stderr = os.Stdin, os.Stdout, os.Stderr
	if err := child.Run(); err != nil {
		return fmt.Errorf("shared PostgreSQL integration command failed: %w", err)
	}
	return nil
}

func environment(values map[string]string) []string {
	env := make([]string, 0, len(os.Environ())+len(values))
	for _, value := range os.Environ() {
		name, _, _ := strings.Cut(value, "=")
		if _, replaced := values[name]; !replaced {
			env = append(env, value)
		}
	}
	for name, value := range values {
		env = append(env, name+"="+value)
	}
	return env
}

func randomHex(bytes int) (string, error) {
	value := make([]byte, bytes)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}
