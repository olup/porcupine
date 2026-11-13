FROM clickhouse/clickhouse-server:26.8.36.3 AS clickhouse
FROM golang:1.26.11-bookworm@sha256:dc40a13b61304647ed7e5d581d246b86413dfc29d63c6d018310feb93cd98fc0 AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY internal ./internal
RUN --mount=type=cache,id=porcupine-analytics-go-mod,target=/go/pkg/mod \
    --mount=type=cache,id=porcupine-analytics-go-build,target=/root/.cache/go-build \
    CGO_ENABLED=1 go test -tags ducklake -c -o /analytics-bench ./internal/porcupine && \
    go version > /go-version.txt && \
    cp -L "$(gcc -print-file-name=libstdc++.so.6)" /libstdc++.so.6
FROM debian:bookworm-slim
COPY --from=clickhouse /usr/bin/clickhouse /usr/bin/clickhouse
COPY --from=clickhouse /etc/clickhouse-server /etc/clickhouse-server
COPY --from=build /analytics-bench /analytics-bench
COPY --from=build /go-version.txt /go-version.txt
COPY --from=build /libstdc++.so.6 /usr/lib/libstdc++.so.6
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY scripts/analytics-benchmark-container.sh /analytics-benchmark-container.sh
ENV GOMAXPROCS=2 TMPDIR=/benchmark-data/tmp
ENTRYPOINT ["sh", "/analytics-benchmark-container.sh"]
