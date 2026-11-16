// shared-postgres-integration runs the shared-state suite against an isolated,
// disposable PostgreSQL server. Its published port is bound only to loopback.
package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const postgresImage = "postgres:17-alpine@sha256:a77aa70e641402a21411b92b721c58d10f23b50ae5fdd2333efd22d166e1e5f7"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pattern := flag.String("run", "^TestShared", "Go test name pattern")
	race := flag.Bool("race", false, "Enable the Go race detector")
	flag.Parse()
	command := flag.Args()
	if len(command) == 0 {
		command = []string{"go", "test", "./internal/porcupine", "-run", *pattern, "-count=1", "-timeout=600s"}
		if *race {
			command = append(command, "-race")
		}
	}
	if err := run(ctx, command); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, command []string) error {
	suffix, err := randomHex(8)
	if err != nil {
		return err
	}
	password, err := randomHex(24)
	if err != nil {
		return err
	}
	container := "porcupine-shared-postgres-" + suffix
	fixtureEnv := environment(map[string]string{"POSTGRES_PASSWORD": password})
	start := exec.CommandContext(ctx, "docker", "run", "-d", "--name", container,
		"--label", "porcupine.integration=true", "--memory", "512m", "--cpus", "1",
		"-p", "127.0.0.1::5432", "-e", "POSTGRES_PASSWORD", "-e", "POSTGRES_DB=porcupine_integration", postgresImage)
	start.Env, start.Stdout, start.Stderr = fixtureEnv, io.Discard, os.Stderr
	if err := start.Run(); err != nil {
		return errors.New("cannot start the isolated PostgreSQL fixture")
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		remove := exec.CommandContext(cleanup, "docker", "rm", "-fv", container)
		remove.Stdout, remove.Stderr = io.Discard, os.Stderr
		remove.Run()
	}()
	port, err := exec.CommandContext(ctx, "docker", "port", container, "5432/tcp").Output()
	if err != nil {
		return errors.New("cannot inspect the isolated PostgreSQL fixture")
	}
	binding := strings.TrimSpace(string(port))
	if !strings.HasPrefix(binding, "127.0.0.1:") || strings.Contains(binding, "\n") {
		return errors.New("PostgreSQL fixture is not bound exclusively to loopback")
	}
	dsn := &url.URL{Scheme: "postgres", User: url.UserPassword("postgres", password), Host: binding, Path: "/porcupine_integration", RawQuery: "sslmode=disable&connect_timeout=1"}
	db, err := sql.Open("pgx", dsn.String())
	if err != nil {
		return errors.New("cannot open the isolated PostgreSQL fixture")
	}
	defer db.Close()
	ready, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for db.PingContext(ready) != nil {
		select {
		case <-ready.Done():
			return errors.New("isolated PostgreSQL fixture did not become ready")
		case <-time.After(100 * time.Millisecond):
		}
	}
	child := exec.CommandContext(ctx, command[0], command[1:]...)
	child.Env = environment(map[string]string{"PORCUPINE_TEST_POSTGRES_DSN": dsn.String(), "CGO_ENABLED": "1"})
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
