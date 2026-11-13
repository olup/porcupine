# syntax=docker/dockerfile:1
FROM golang:1.26.11-bookworm@sha256:dc40a13b61304647ed7e5d581d246b86413dfc29d63c6d018310feb93cd98fc0 AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY cmd ./cmd
COPY internal ./internal
ARG VERSION=dev
RUN --mount=type=cache,id=porcupine-analytics-go-mod,target=/go/pkg/mod \
    --mount=type=cache,id=porcupine-analytics-go-build,target=/root/.cache/go-build \
    CGO_ENABLED=1 go build -tags ducklake -trimpath \
    -ldflags="-s -w -X github.com/olup/porcupine/internal/porcupine.Version=$VERSION" \
    -o /porcupine ./cmd/porcupine && mkdir /data
RUN --mount=type=cache,id=porcupine-analytics-go-mod,target=/go/pkg/mod \
    --mount=type=cache,id=porcupine-analytics-go-build,target=/root/.cache/go-build <<'EOF'
cat > /tmp/extension.go <<'GO'
package main
import (
    "database/sql"
    "log"
    _ "github.com/duckdb/duckdb-go/v2"
)
func main() {
    db, err := sql.Open("duckdb", "")
    if err != nil { log.Fatal(err) }
    defer db.Close()
    if _, err := db.Exec("INSTALL ducklake; LOAD ducklake"); err != nil { log.Fatal(err) }
}
GO
CGO_ENABLED=1 go run /tmp/extension.go
chmod -R a+rX /root/.duckdb/extensions
EOF

FROM gcr.io/distroless/cc-debian12:nonroot@sha256:9dac0a79194e45a7da0158a9c6da57b217585af0786db3845d1f0ec1a0dd182f
COPY --from=build /porcupine /porcupine
COPY --from=build --chown=65532:65532 /data /data
COPY --from=build /root/.duckdb/extensions /home/nonroot/.duckdb/extensions
ENV HOME=/home/nonroot PORCUPINE_ADDR=0.0.0.0:8080 PORCUPINE_DB=/data/porcupine.db PORCUPINE_DUCKLAKE_DIR=/data/ducklake
EXPOSE 8080
USER 65532:65532
ENTRYPOINT ["/porcupine"]
