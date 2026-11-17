package porcupine

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// pitrPostgres owns both clusters and their WAL archive. No external cluster URL
// is accepted by the fixture.
type pitrPostgres struct {
	t                                         *testing.T
	bin, primary, restored, archive, password string
	port                                      string
	running                                   string
}

func newPITRPostgres(t *testing.T, bin string) *pitrPostgres {
	t.Helper()
	root := t.TempDir()
	p := &pitrPostgres{t: t, bin: bin, primary: filepath.Join(root, "primary"),
		restored: filepath.Join(root, "restored"), archive: filepath.Join(root, "archive")}
	t.Cleanup(func() {
		if p.running != "" {
			p.run("pg_ctl", "-D", p.running, "-m", "immediate", "-w", "stop")
		}
	})
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p.port = strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
	listener.Close()
	secret := make([]byte, 24)
	if _, err := rand.Read(secret); err != nil {
		t.Fatal(err)
	}
	p.password = hex.EncodeToString(secret)
	passwordFile := filepath.Join(root, "password")
	if err := os.WriteFile(passwordFile, []byte(p.password+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(p.archive, 0700); err != nil {
		t.Fatal(err)
	}
	p.run("initdb", "-D", p.primary, "-U", "postgres", "--auth=scram-sha-256", "--pwfile", passwordFile, "--no-locale", "--encoding=UTF8")
	p.configure(p.primary, "listen_addresses = '127.0.0.1'\n"+
		"unix_socket_directories = ''\nport = "+p.port+"\nshared_buffers = '32MB'\n"+
		"archive_mode = on\narchive_command = "+pitrSQLLiteral("cp "+pitrShellQuote("%p")+" "+pitrShellQuote(filepath.Join(p.archive, "%f")))+"\n")
	p.start(p.primary)
	return p
}

func (p *pitrPostgres) run(program string, args ...string) string {
	p.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, filepath.Join(p.bin, program), args...)
	cmd.Env = append(os.Environ(), "PGPASSWORD="+p.password, "PGCONNECT_TIMEOUT=5")
	b, err := cmd.CombinedOutput()
	if err != nil {
		p.t.Fatalf("%s: %v\n%s", program, err, strings.ReplaceAll(string(b), p.password, "[redacted]"))
	}
	return string(b)
}

func (p *pitrPostgres) start(directory string) {
	p.t.Helper()
	p.run("pg_ctl", "-D", directory, "-l", directory+".log", "-w", "start")
	p.running = directory
}

func (p *pitrPostgres) configure(directory, config string) {
	p.t.Helper()
	f, err := os.OpenFile(filepath.Join(directory, "postgresql.conf"), os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		p.t.Fatal(err)
	}
	_, writeErr := f.WriteString("\n" + config)
	closeErr := f.Close()
	if writeErr != nil || closeErr != nil {
		p.t.Fatalf("write PostgreSQL configuration: %v %v", writeErr, closeErr)
	}
}

func (p *pitrPostgres) dsn(database string) string {
	u := url.URL{Scheme: "postgres", Host: net.JoinHostPort("127.0.0.1", p.port), Path: "/" + database,
		User: url.UserPassword("postgres", p.password), RawQuery: "sslmode=disable&connect_timeout=5"}
	return u.String()
}

func (p *pitrPostgres) admin() *sql.DB {
	p.t.Helper()
	db, err := sql.Open("pgx", p.dsn("postgres"))
	if err != nil {
		p.t.Fatal(err)
	}
	p.t.Cleanup(func() { db.Close() })
	return db
}

func (p *pitrPostgres) backup() {
	p.t.Helper()
	p.run("pg_basebackup", "-h", "127.0.0.1", "-p", p.port, "-U", "postgres", "-D", p.restored, "--checkpoint=fast", "--wal-method=none", "--no-password")
}

func (p *pitrPostgres) recover(target string) {
	p.t.Helper()
	db := p.admin()
	var lastSegment string
	// Force a segment boundary after post-target transactions and wait for the
	// archiver to finish copying it before taking the primary offline.
	if err := db.QueryRow("SELECT pg_walfile_name(pg_switch_wal())").Scan(&lastSegment); err != nil {
		p.t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		var archived bool
		err := db.QueryRow("SELECT COALESCE(last_archived_wal = $1, false) FROM pg_stat_archiver", lastSegment).Scan(&archived)
		if err == nil && archived {
			break
		}
		if time.Now().After(deadline) {
			p.t.Fatalf("WAL segment %s not archived: %v", lastSegment, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	p.run("pg_ctl", "-D", p.primary, "-m", "fast", "-w", "stop")
	p.running = ""
	p.configure(p.restored, "archive_mode = off\n"+
		"restore_command = "+pitrSQLLiteral("cp "+pitrShellQuote(filepath.Join(p.archive, "%f"))+" "+pitrShellQuote("%p"))+"\n"+
		"recovery_target_name = "+pitrSQLLiteral(target)+"\nrecovery_target_action = 'promote'\n")
	if err := os.WriteFile(filepath.Join(p.restored, "recovery.signal"), nil, 0600); err != nil {
		p.t.Fatal(err)
	}
	p.start(p.restored)
	restored := p.admin()
	// pg_ctl can return as soon as hot standby accepts read-only connections.
	ctx, cancel := context.WithTimeout(p.t.Context(), 15*time.Second)
	defer cancel()
	var writable bool
	for ctx.Err() == nil {
		if err := restored.QueryRowContext(ctx, "SELECT NOT pg_is_in_recovery()").Scan(&writable); err == nil && writable {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	log, err := os.ReadFile(p.restored + ".log")
	if err != nil || !writable || !strings.Contains(string(log), fmt.Sprintf("recovery stopping at restore point \"%s\"", target)) {
		p.t.Fatalf("PostgreSQL did not report reaching the named restore point: %v\n%s", err, log)
	}
}

func pitrShellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

func pitrSQLLiteral(value string) string {
	return "'" + strings.ReplaceAll(strings.ReplaceAll(value, "\\", "\\\\"), "'", "''") + "'"
}
