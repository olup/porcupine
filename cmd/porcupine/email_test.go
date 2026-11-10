package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http/httptest"
	"net/mail"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/emersion/go-sasl"
	"github.com/emersion/go-smtp"
)

type mailSession struct {
	delivered chan<- string
	secure    bool
	authed    bool
}

func (s *mailSession) Reset()                   {}
func (s *mailSession) Logout() error            { return nil }
func (s *mailSession) AuthMechanisms() []string { return []string{"PLAIN"} }
func (s *mailSession) Auth(mech string) (sasl.Server, error) {
	if !s.secure || mech != "PLAIN" {
		return nil, smtp.ErrAuthFailed
	}
	return sasl.NewPlainServer(func(identity, user, password string) error {
		if identity != "" || user != "mailer" || password != "smtp-test-password" {
			return smtp.ErrAuthFailed
		}
		s.authed = true
		return nil
	}), nil
}
func (s *mailSession) Mail(from string, _ *smtp.MailOptions) error {
	if !s.authed || from != "porcupine@example.test" {
		return smtp.ErrAuthRequired
	}
	return nil
}
func (s *mailSession) Rcpt(to string, _ *smtp.RcptOptions) error {
	if to != "lea@example.test" {
		return errors.New("unexpected recipient")
	}
	return nil
}
func (s *mailSession) Data(r io.Reader) error {
	data, err := io.ReadAll(r)
	if err == nil {
		s.delivered <- string(data)
	}
	return err
}

func TestVerificationEmailDelivery(t *testing.T) {
	testTLS := httptest.NewTLSServer(nil)
	defer testTLS.Close()
	roots := x509.NewCertPool()
	roots.AddCert(testTLS.Certificate())
	for _, mode := range []string{"starttls", "tls", "cleartext relay", "untrusted relay"} {
		t.Run(mode, func(t *testing.T) {
			delivered := make(chan string, 1)
			server := smtp.NewServer(smtp.BackendFunc(func(conn *smtp.Conn) (smtp.Session, error) {
				_, isTLS := conn.TLSConnectionState()
				return &mailSession{secure: isTLS, delivered: delivered}, nil
			}))
			server.Domain = "localhost"
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			if mode != "cleartext relay" {
				server.TLSConfig = testTLS.TLS.Clone()
			}
			if mode == "tls" {
				listener = tls.NewListener(listener, server.TLSConfig)
			}
			done := make(chan error, 1)
			go func() { done <- server.Serve(listener) }()
			defer func() {
				_ = server.Close()
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			}()

			transport := smtpDelivery{
				issuer:   "https://analytics.example.test",
				address:  listener.Addr().String(),
				tls:      &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: "127.0.0.1"},
				implicit: mode == "tls", user: "mailer", password: "smtp-test-password",
				from: &mail.Address{Address: "porcupine@example.test", Name: "Porcupine"},
			}
			if mode == "untrusted relay" {
				transport.tls.RootCAs = x509.NewCertPool()
			}
			err = transport.sendCode(context.Background(), "lea@example.test", "135790")
			if mode == "cleartext relay" || mode == "untrusted relay" {
				if err == nil || len(delivered) != 0 {
					t.Fatal("delivered OTP to untrusted relay")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			select {
			case raw := <-delivered:
				msg, err := mail.ReadMessage(strings.NewReader(raw))
				if err != nil {
					t.Fatal(err)
				}
				to, err := msg.Header.AddressList("To")
				if err != nil || len(to) != 1 || to[0].Address != "lea@example.test" {
					t.Fatal("wrong message destination")
				}
				b, err := io.ReadAll(msg.Body)
				if err != nil || !strings.Contains(string(b), "135790") || strings.Contains(raw, transport.password) {
					t.Fatal("wrong verification email contents")
				}
			default:
				t.Fatal("no message")
			}
			if err := transport.sendInvitation(context.Background(), "lea@example.test", false); err != nil {
				t.Fatal(err)
			}
			notice := <-delivered
			if !strings.Contains(notice, "Subject: You have been invited to Porcupine") || !strings.Contains(notice, transport.issuer) || !strings.Contains(notice, "request a verification code") || strings.Contains(notice, "135790") || strings.Contains(notice, transport.password) {
				t.Fatal("invalid invitation notice")
			}
			if err := transport.sendInvitation(context.Background(), "lea@example.test", true); err != nil {
				t.Fatal(err)
			}
			adminNotice := <-delivered
			if !strings.Contains(adminNotice, "Subject: Invitation to administer Porcupine") || !strings.Contains(adminNotice, "porcupine-local-admin") || !strings.Contains(adminNotice, "instance:admin") || strings.Contains(notice, "instance:admin") {
				t.Fatal("incorrect role in invitation notice")
			}

		})
	}
}

func TestSMTPContextCancellation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sender := smtpDelivery{address: listener.Addr().String(), tls: &tls.Config{ServerName: "localhost", MinVersion: tls.VersionTLS12}}
	done := make(chan error, 1)
	go func() { done <- sender.sendCode(ctx, "lea@example.test", "123456") }()
	select {
	case conn := <-accepted:
		defer conn.Close()
	case <-time.After(2 * time.Second):
		t.Fatal("connection not opened")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("accepted canceled send")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked waiting for SMTP greeting after cancel")
	}
}

func TestSMTPConfiguration(t *testing.T) {
	for _, name := range []string{"PORCUPINE_SMTP_HOST", "PORCUPINE_SMTP_PORT", "PORCUPINE_SMTP_TLS", "PORCUPINE_SMTP_FROM", "PORCUPINE_SMTP_USERNAME", "PORCUPINE_SMTP_PASSWORD_FILE", "PORCUPINE_ADMIN_EMAIL", "PORCUPINE_OAUTH_ISSUER"} {
		t.Setenv(name, "")
	}
	if config, err := emailConfiguration(); err != nil || config != nil {
		t.Fatal("SMTP should be opt-in")
	}
	t.Setenv("PORCUPINE_SMTP_HOST", "smtp.example.test")
	t.Setenv("PORCUPINE_SMTP_FROM", "Porcupine <porcupine@example.test>")
	if _, err := emailConfiguration(); err == nil {
		t.Fatal("missing OAuth issuer")
	}
	t.Setenv("PORCUPINE_OAUTH_ISSUER", "https://analytics.example.test")
	t.Setenv("PORCUPINE_ADMIN_EMAIL", "operator@example.test")
	if c, err := emailConfiguration(); err != nil || c.SendCode == nil || c.AdminEmail != "operator@example.test" {
		t.Fatal("valid TLS relay settings rejected")
	}
	for name, value := range map[string]string{"PORCUPINE_SMTP_HOST": "https://smtp.example.test", "PORCUPINE_SMTP_PORT": "0", "PORCUPINE_SMTP_TLS": "off", "PORCUPINE_SMTP_FROM": "a@example.test\r\nBcc: b@example.test", "PORCUPINE_SMTP_USERNAME": "unpaired", "PORCUPINE_SMTP_PASSWORD_FILE": "unpaired"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, value)
			if _, err := emailConfiguration(); err == nil {
				t.Fatal("invalid SMTP configuration was accepted")
			}
		})
	}
	f := filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(f, []byte("a-test-secret\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PORCUPINE_SMTP_USERNAME", "mailer")
	t.Setenv("PORCUPINE_SMTP_PASSWORD_FILE", f)
	if _, err := emailConfiguration(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f, []byte("bad\x00secret"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := emailConfiguration(); err == nil {
		t.Fatal("invalid secret was accepted")
	}
}
