package main

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/signal"
	"strings"

	"github.com/urfave/cli/v3"
)

var version = "dev"

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	if err := newApp().Run(ctx, os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func newApp() *cli.Command {
	app := &cli.Command{
		Name: "porcupine", Version: version, Usage: "Query and manage a Porcupine instance",
		Flags: []cli.Flag{&cli.StringFlag{Name: "url", Usage: "Instance origin", Sources: cli.EnvVars("PORCUPINE_URL"), Value: "http://127.0.0.1:8080"}},
		Commands: []*cli.Command{
			restoreCommand(),
			importCommand(),
			visibilityProbeCommand(),
			{Name: "login", Usage: "Authorize this CLI in your browser", Flags: []cli.Flag{
				&cli.BoolFlag{Name: "admin", Usage: "Request instance administrator access"},
				&cli.BoolFlag{Name: "tenant-admin", Usage: "Request administration of one tenant selected in your browser"},
				&cli.StringFlag{Name: "scopes", Usage: "Project scopes to request, separated by spaces or commas; defaults to read-only"},
				&cli.StringFlag{Name: "client-id", Usage: "Registered public OAuth client ID for an external identity provider", Sources: cli.EnvVars("PORCUPINE_OAUTH_CLIENT_ID")},
			}, Action: func(ctx context.Context, c *cli.Command) error {
				origin, err := instanceOrigin(c)
				if err != nil {
					return err
				}
				clientID, scopes, err := loginOptions(c.Bool("admin"), c.Bool("tenant-admin"), c.String("scopes"))
				if err != nil {
					return err
				}
				return login(ctx, origin, clientID, scopes, c.String("client-id"))
			}},
			{Name: "status", Usage: "Check instance readiness", Action: func(ctx context.Context, c *cli.Command) error {
				return runOperation(ctx, c, operation{Method: "GET", Path: "/readyz"}, false)
			}},
		},
	}
	for _, op := range operations {
		app.Commands = append(app.Commands, apiCommand(op))
	}
	return app
}

func instanceOrigin(c *cli.Command) (string, error) {
	if c.NArg() != 0 {
		return "", fmt.Errorf("unexpected positional arguments, see %s --help", c.Name)
	}
	return parseInstanceOrigin(c.Root().String("url"))
}

func parseInstanceOrigin(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || u.User != nil || u.EscapedPath() != "" && u.EscapedPath() != "/" || strings.ContainsAny(raw, "?#") {
		return "", fmt.Errorf("--url requires an instance origin such as https://analytics.example.com")
	}
	ip := net.ParseIP(u.Hostname())
	local := strings.EqualFold(u.Hostname(), "localhost") || ip != nil && ip.IsLoopback()
	if u.Scheme != "https" && !(u.Scheme == "http" && local) {
		return "", fmt.Errorf("--url requires HTTPS or an HTTP loopback address")
	}
	return u.Scheme + "://" + strings.ToLower(u.Host), nil
}
