package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/mail"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/emersion/go-sasl"
	"github.com/emersion/go-smtp"
	"github.com/olup/porcupine/internal/porcupine"
)

type smtpDelivery struct {
	issuer   string
	address  string
	tls      *tls.Config
	implicit bool
	user     string
	password string
	from     *mail.Address
}

func emailConfiguration() (*porcupine.EmailConfig, error) {
	host, port, mode := os.Getenv("PORCUPINE_SMTP_HOST"), os.Getenv("PORCUPINE_SMTP_PORT"), os.Getenv("PORCUPINE_SMTP_TLS")
	from := os.Getenv("PORCUPINE_SMTP_FROM")
	user, secretFile := os.Getenv("PORCUPINE_SMTP_USERNAME"), os.Getenv("PORCUPINE_SMTP_PASSWORD_FILE")
	admin := os.Getenv("PORCUPINE_ADMIN_EMAIL")
	if host == "" && port == "" && mode == "" && from == "" && user == "" && secretFile == "" && admin == "" {
		return nil, nil
	}
	if os.Getenv("PORCUPINE_OAUTH_ISSUER") == "" || host == "" || from == "" {
		return nil, errors.New("email login requires PORCUPINE_OAUTH_ISSUER, PORCUPINE_SMTP_HOST and PORCUPINE_SMTP_FROM")
	}
	if strings.ContainsAny(host, "/\\[]@ \r\n\t?#") || strings.Contains(host, ":") && net.ParseIP(host) == nil {
		return nil, errors.New("PORCUPINE_SMTP_HOST must be a hostname or IP address")
	}
	if mode != "" && mode != "starttls" && mode != "tls" {
		return nil, errors.New("PORCUPINE_SMTP_TLS must be starttls or tls")
	}
	if port == "" {
		port = "587"
		if mode == "tls" {
			port = "465"
		}
	}
	portNum, err := strconv.Atoi(port)
	if err != nil || portNum < 1 || portNum > 65535 {
		return nil, errors.New("invalid PORCUPINE_SMTP_PORT")
	}
	address, err := mail.ParseAddress(from)
	if err != nil || strings.ContainsAny(from, "\r\n") {
		return nil, errors.New("invalid PORCUPINE_SMTP_FROM")
	}
	if (user == "") != (secretFile == "") || strings.ContainsAny(user, "\x00\r\n") {
		return nil, errors.New("SMTP authentication requires both PORCUPINE_SMTP_USERNAME and PORCUPINE_SMTP_PASSWORD_FILE")
	}
	sender := smtpDelivery{
		issuer:   strings.TrimSuffix(os.Getenv("PORCUPINE_OAUTH_ISSUER"), "/"),
		address:  net.JoinHostPort(host, strconv.Itoa(portNum)),
		tls:      &tls.Config{MinVersion: tls.VersionTLS12, ServerName: host},
		implicit: mode == "tls", user: user, from: address,
	}
	if secretFile != "" {
		f, err := os.Open(secretFile)
		if err != nil {
			return nil, errors.New("cannot open PORCUPINE_SMTP_PASSWORD_FILE")
		}
		defer f.Close()
		const max = 16 << 10
		raw, err := io.ReadAll(io.LimitReader(f, max+1))
		sender.password = strings.TrimRight(string(raw), "\r\n")
		if err != nil || len(raw) > max || sender.password == "" || strings.ContainsAny(sender.password, "\r\n\x00") {
			return nil, errors.New("invalid PORCUPINE_SMTP_PASSWORD_FILE")
		}
	}
	return &porcupine.EmailConfig{SendCode: sender.sendCode, SendInvitation: sender.sendInvitation, AdminEmail: admin}, nil
}

func (s smtpDelivery) sendCode(ctx context.Context, recipient, code string) error {
	body := fmt.Sprintf("Your verification code for %s is %s.", s.issuer, code)
	return s.send(ctx, recipient, "Your Porcupine verification code", body)
}

func (s smtpDelivery) sendInvitation(ctx context.Context, recipient string, instanceAdmin bool) error {
	if instanceAdmin {
		body := fmt.Sprintf("You have been invited to administer the Porcupine instance at %s.\r\n\r\nFor a local MCP client, use %s/mcp, client ID porcupine-local-admin, callback http://127.0.0.1:43179/callback and scope instance:admin. In Porcupine, enter this email address to request a verification code, then review and approve access to all projects.", s.issuer, s.issuer)
		return s.send(ctx, recipient, "Invitation to administer Porcupine", body)
	}
	body := fmt.Sprintf("You have been invited to Porcupine at %s.\r\n\r\nIn your team's approved analytics tool, choose Connect for this Porcupine instance. Enter your email address to request a verification code, then review the project access requested by the tool.", s.issuer)
	return s.send(ctx, recipient, "You have been invited to Porcupine", body)
}

func (s smtpDelivery) send(ctx context.Context, recipient, subject, body string) error {
	to, err := mail.ParseAddress(recipient)
	if err != nil || to.Address != recipient || strings.ContainsAny(recipient, "\r\n") {
		return errors.New("invalid email recipient")
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", s.address)
	if err != nil {
		return errors.New("cannot connect to SMTP relay")
	}
	defer conn.Close()
	// The SMTP client sets per-command deadlines. Closing the socket also bounds the entire transaction.
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	var client *smtp.Client
	if s.implicit {
		client = smtp.NewClient(tls.Client(conn, s.tls))
	} else {
		client, err = smtp.NewClientStartTLS(conn, s.tls)
		if err != nil {
			return errors.New("SMTP relay failed required STARTTLS")
		}
	}
	if s.user != "" {
		var auth sasl.Client
		switch {
		case client.SupportsAuth("PLAIN"):
			auth = sasl.NewPlainClient("", s.user, s.password)
		case client.SupportsAuth("LOGIN"):
			auth = sasl.NewLoginClient(s.user, s.password)
		default:
			return errors.New("SMTP relay requires a supported authentication mechanism")
		}
		if err := client.Auth(auth); err != nil {
			return errors.New("SMTP relay rejected authentication")
		}
	}
	domain := s.from.Address[strings.LastIndexByte(s.from.Address, '@')+1:]
	message := fmt.Sprintf("Message-ID: <%s@%s>\r\nFrom: %s\r\nTo: %s\r\nDate: %s\r\nSubject: %s\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n%s\r\n", rand.Text(), domain, s.from.String(), to.String(), time.Now().UTC().Format(time.RFC1123Z), subject, body)
	if err := client.SendMail(s.from.Address, []string{to.Address}, strings.NewReader(message)); err != nil {
		return errors.New("SMTP relay did not accept the email")
	}
	// DATA was acknowledged. A disconnect during QUIT cannot undo delivery.
	_ = client.Quit()
	return nil
}
