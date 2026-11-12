FROM --platform=$BUILDPLATFORM golang:1.26.11-bookworm@sha256:dc40a13b61304647ed7e5d581d246b86413dfc29d63c6d018310feb93cd98fc0 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath \
    -ldflags="-s -w -X github.com/olup/porcupine/internal/porcupine.Version=$VERSION" \
    -o /porcupine ./cmd/porcupine && mkdir /data

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=build /porcupine /porcupine
COPY --from=build --chown=65532:65532 /data /data
ENV PORCUPINE_ADDR=0.0.0.0:8080 PORCUPINE_DB=/data/porcupine.db
EXPOSE 8080
USER 65532:65532
ENTRYPOINT ["/porcupine"]
