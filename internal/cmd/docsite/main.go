// Command docsite builds and previews the website with a pinned Hugo binary.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/olup/porcupine/internal/docsite"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		return fmt.Errorf("usage: docsite build | preview [--port 8787] | check-release [--tag vX.Y.Z] | published-release")
	}
	root, err := docsite.FindRoot(".")
	if err != nil {
		return err
	}
	ctx := context.Background()
	switch os.Args[1] {
	case "build":
		if err := docsite.Build(ctx, root); err != nil {
			return err
		}
		fmt.Println("Built landing, HTML guides and Markdown in bin/docs-web/porcupine")
		return nil
	case "check-release":
		flags := flag.NewFlagSet("check-release", flag.ContinueOnError)
		tag := flags.String("tag", "", "release tag to verify")
		if err := flags.Parse(os.Args[2:]); err != nil {
			return err
		}
		version, err := docsite.CheckRelease(root, *tag)
		if err != nil {
			return err
		}
		fmt.Println("Release pointers match v" + version)
		return nil
	case "published-release":
		version, err := docsite.ReleaseVersion(root)
		if err != nil {
			return err
		}
		published, err := docsite.PublishedRelease(ctx, nil, os.Getenv("GITHUB_API_URL"), os.Getenv("GITHUB_REPOSITORY"), os.Getenv("GH_TOKEN"), version)
		if err != nil {
			return err
		}
		if output := os.Getenv("GITHUB_OUTPUT"); output != "" {
			file, err := os.OpenFile(output, os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(file, "published=%t\n", published)
			closeErr := file.Close()
			if err != nil {
				return err
			}
			if closeErr != nil {
				return closeErr
			}
		}
		fmt.Printf("v%s published=%t\n", version, published)
		return nil
	case "preview":
		flags := flag.NewFlagSet("preview", flag.ContinueOnError)
		port := flags.Int("port", 8787, "local preview port")
		if err := flags.Parse(os.Args[2:]); err != nil {
			return err
		}
		fmt.Printf("Documentation: http://127.0.0.1:%d/porcupine/docs/\n", *port)
		server := http.Server{Addr: fmt.Sprintf("127.0.0.1:%d", *port), Handler: docsite.PreviewHandler(filepath.Join(root, "bin/docs-web")), ReadHeaderTimeout: 5 * time.Second}
		return server.ListenAndServe()
	default:
		return fmt.Errorf("unknown docsite command %q", os.Args[1])
	}
}
