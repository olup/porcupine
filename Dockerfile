FROM golang:1.26.10-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /porcupine ./cmd/porcupine \
    && mkdir /data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /porcupine /porcupine
COPY --from=build --chown=65532:65532 /data /data
ENV PORCUPINE_ADDR=0.0.0.0:8080 PORCUPINE_DB=/data/porcupine.db
EXPOSE 8080
USER 65532:65532
ENTRYPOINT ["/porcupine"]
