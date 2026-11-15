// Command genapi snapshots the authoritative Huma management contract without
// opening a database or starting the application.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"log"
	"os"

	"github.com/olup/porcupine/internal/porcupine"
)

func document() ([]byte, error) {
	data, err := json.MarshalIndent(porcupine.ManagementOpenAPI(), "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func main() {
	output := flag.String("o", "internal/apischema/management.openapi.json", "output OpenAPI JSON")
	check := flag.Bool("check", false, "fail if the generated contract is out of date")
	flag.Parse()
	log.SetFlags(0)
	data, err := document()
	if err != nil {
		log.Fatal(err)
	}
	if *check {
		current, err := os.ReadFile(*output)
		if err != nil {
			log.Fatal(err)
		}
		if !bytes.Equal(current, data) {
			log.Fatalf("%s is stale; run go run ./internal/cmd/genapi", *output)
		}
		return
	}
	if err := os.WriteFile(*output, data, 0644); err != nil {
		log.Fatal(err)
	}
}
