package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/urfave/cli/v3"
)

type operation struct {
	ID, Method, Path, Summary, ContentType string
	Params                                 []parameter
	BodyRequired                           bool
	BodyFields                             []string
}

type parameter struct {
	Name, In, Type, Description string
	Required                    bool
}

func (p parameter) flagName() string {
	return strings.ToLower(strings.ReplaceAll(p.Name, "_", "-"))
}

func apiCommand(op operation) *cli.Command {
	cmd := &cli.Command{Name: op.ID, Usage: op.Summary, Category: "Management API", Description: op.Method + " " + op.Path}
	for _, p := range op.Params {
		usage := p.Description
		if usage == "" {
			usage = p.Name + " (" + p.In + ", " + p.Type + ")"
		}
		cmd.Flags = append(cmd.Flags, &cli.StringFlag{Name: p.flagName(), Usage: usage, Required: p.Required})
	}
	if op.ContentType != "" {
		usage := "JSON, @file or @- for stdin"
		if op.ContentType == "application/octet-stream" {
			usage = "Bytes from @file or @- for stdin"
		}
		cmd.Flags = append(cmd.Flags, &cli.StringFlag{Name: "body", Usage: usage, Required: op.BodyRequired})
		if len(op.BodyFields) > 0 {
			cmd.Description += "\n\nBody fields\n  " + strings.Join(op.BodyFields, "\n  ")
		}
	}
	cmd.Action = func(ctx context.Context, c *cli.Command) error { return runOperation(ctx, c, op, true) }
	return cmd
}

func requestBody(c *cli.Command, op operation) (io.ReadCloser, error) {
	if !c.IsSet("body") {
		return nil, nil
	}
	value := c.String("body")
	switch {
	case value == "@-":
		return io.NopCloser(c.Root().Reader), nil
	case strings.HasPrefix(value, "@"):
		return os.Open(value[1:])
	case op.ContentType == "application/json":
		return io.NopCloser(strings.NewReader(value)), nil
	default:
		return nil, fmt.Errorf("--body requires @file or @- for binary content")
	}
}

func buildRequest(ctx context.Context, c *cli.Command, op operation, origin string) (*http.Request, error) {
	path := op.Path
	query := url.Values{}
	headers := http.Header{}
	for _, p := range op.Params {
		if !c.IsSet(p.flagName()) {
			continue
		}
		value := c.String(p.flagName())
		if p.Type == "integer" {
			if _, err := strconv.ParseInt(value, 10, 64); err != nil {
				return nil, fmt.Errorf("--%s requires an integer", p.flagName())
			}
		}
		switch p.In {
		case "path":
			if value == "" || value == "." || value == ".." || strings.ContainsAny(value, "/\\") {
				return nil, fmt.Errorf("--%s requires a single path segment", p.flagName())
			}
			path = strings.ReplaceAll(path, "{"+p.Name+"}", url.PathEscape(value))
		case "query":
			query.Set(p.Name, value)
		case "header":
			headers.Set(p.Name, value)
		}
	}
	endpoint := origin + path
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, op.Method, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header = headers
	req.Header.Set("Accept", "application/json")
	body, err := requestBody(c, op)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Body = body
		req.Header.Set("Content-Type", op.ContentType)
	}
	return req, nil
}

func runOperation(ctx context.Context, c *cli.Command, op operation, authenticated bool) error {
	origin, err := instanceOrigin(c)
	if err != nil {
		return err
	}
	req, err := buildRequest(ctx, c, op, origin)
	if err != nil {
		return err
	}
	if req.Body != nil {
		defer req.Body.Close()
	}
	if authenticated {
		token := os.Getenv("PORCUPINE_API_KEY")
		if token == "" {
			token, err = accessToken(ctx, origin)
			if err != nil {
				return err
			}
		}
		req.Header.Set("Authorization", "Bearer "+token)
	}
	client := &http.Client{Timeout: 2 * time.Minute, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return responseError(op, resp, req.Header.Get("Authorization"))
	}
	_, err = io.Copy(c.Root().Writer, resp.Body)
	return err
}

// responseError reads the API message and its documented recovery fields.
// Headers, HTML pages and other JSON fields can contain credentials or payloads.
func responseError(op operation, resp *http.Response, authorization string) error {
	status := fmt.Sprintf("%s %s returned HTTP %d (%s)", op.Method, op.Path, resp.StatusCode, http.StatusText(resp.StatusCode))
	fallback := fmt.Errorf("%s", status)
	media, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || media != "application/json" && media != "application/problem+json" {
		return fallback
	}
	const maxErrorBytes = 64 << 10
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxErrorBytes+1))
	if err != nil || len(raw) > maxErrorBytes {
		return fallback
	}
	var body struct {
		Error  string `json:"error"`
		Detail string `json:"detail"`
		Title  string `json:"title"`
		Code   string `json:"code"`
		Hint   string `json:"hint"`
		Field  string `json:"field"`
	}
	if json.Unmarshal(raw, &body) != nil {
		return fallback
	}
	message := body.Error
	if media == "application/problem+json" && message == "" {
		message = body.Detail
		if message == "" {
			message = body.Title
		}
	}
	if strings.TrimSpace(message) != "" {
		if body.Code != "" {
			message += " [" + body.Code + "]"
		}
		if body.Field != "" {
			message += " field: " + body.Field
		}
		if body.Hint != "" {
			message += ". " + body.Hint
		}
	}
	if credential := strings.TrimPrefix(authorization, "Bearer "); credential != "" {
		message = strings.ReplaceAll(message, credential, "[redacted]")
	}
	message = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || unicode.In(r, unicode.Cf, unicode.Zl, unicode.Zp) {
			return ' '
		}
		return r
	}, message)
	message = strings.Join(strings.Fields(message), " ")
	const maxMessageRunes = 1024
	if runes := []rune(message); len(runes) > maxMessageRunes {
		message = string(runes[:maxMessageRunes]) + "…"
	}
	if message == "" {
		return fallback
	}
	return fmt.Errorf("%s: %s", status, message)
}
