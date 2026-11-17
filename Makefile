.PHONY: build run test test-core test-full test-race check check-full lint fmt integration integration-s3

# Examples: make test PKG=./internal/porcupine RUN=TestTenant
#           make test-core RUN=TestGuard
PKG ?= ./...
RUN ?= .
CORE_PKGS := ./internal/auth ./internal/delivery ./internal/event ./internal/importer ./internal/ingest ./internal/s3client ./internal/exportstore ./internal/nativequery ./internal/apierr ./internal/syncx ./internal/docsite

build:
	CGO_ENABLED=1 go build -trimpath -o bin/porcupine ./cmd/porcupine

run:
	CGO_ENABLED=1 go run ./cmd/porcupine

# Fast loop: skips the slowest journeys (testing.Short). CI runs everything.
test:
	CGO_ENABLED=1 go test -short $(PKG) -run '$(RUN)' -timeout=300s

# Pure Go rules and parsers can be checked without linking the native engine.
test-core:
	CGO_ENABLED=0 go test $(CORE_PKGS) -run '$(RUN)' -timeout=60s

test-full:
	CGO_ENABLED=1 go test $(PKG) -run '$(RUN)' -timeout=600s

test-race:
	CGO_ENABLED=1 go test -race $(PKG) -run '$(RUN)' -count=1 -timeout=600s

lint: api-check cli-check
	CGO_ENABLED=1 go vet ./...
	@test -z "$$(gofmt -l cmd internal)" || (echo 'Run make fmt'; exit 1)

# check is the local loop; check-full is what CI requires before merge.
check: lint test

check-full: lint test-race

fmt:
	gofmt -w cmd internal cli/cmd

integration: ducklake-smoke ducklake-s3-test ducklake-restore-drill shared-integration

integration-s3: ducklake-s3-test

.PHONY: compat-install compat compat-canary
compat-install:
	npm ci --prefix compat
	npm --prefix compat run install-browser

compat:
	npm --prefix compat test

# Run in a disposable checkout; the lockfile remains the supported SDK baseline.
compat-canary:
	cd compat && npm install --no-save --package-lock=false posthog-js@latest posthog-node@latest
	PORCUPINE_COMPAT_CANARY=1 npm --prefix compat test

.PHONY: security-check restore-drill
security-check:
	CGO_ENABLED=1 go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...
	CGO_ENABLED=1 go test ./internal/porcupine -run '^$$' -fuzz '^FuzzValidateNativeSQL$$' -fuzztime=10s -parallel=2
	CGO_ENABLED=1 go test ./internal/porcupine -run '^$$' -fuzz '^FuzzCaptureBatch$$' -fuzztime=10s -parallel=2

restore-drill: ducklake-restore-drill

.PHONY: api-generate api-check cli cli-check cli-generate
api-generate:
	CGO_ENABLED=1 go run ./internal/cmd/genapi

api-check:
	CGO_ENABLED=1 go run ./internal/cmd/genapi -check

cli:
	cd cli && CGO_ENABLED=0 go build -trimpath -o ../bin/cli/porcupine ./cmd/porcupine

cli-generate: api-generate
	CGO_ENABLED=0 go run ./internal/cmd/gencli

cli-check:
	CGO_ENABLED=0 go run ./internal/cmd/gencli -check
	cd cli && CGO_ENABLED=0 go vet ./... && CGO_ENABLED=0 go test ./...
	@test -z "$$(gofmt -l cli/cmd)" || (echo 'Run make fmt'; exit 1)

.PHONY: docs docs-preview
docs:
	CGO_ENABLED=0 go run ./internal/cmd/docsite build

docs-preview: docs
	CGO_ENABLED=0 go run ./internal/cmd/docsite preview

.PHONY: ducklake-build ducklake-test ducklake-smoke ducklake-bench ducklake-batching-bench ducklake-architecture-test
ducklake-build: build

ducklake-test: test-race

ducklake-smoke: build cli
	CGO_ENABLED=0 go run ./internal/cmd/ducklake-smoke -server ./bin/porcupine -cli ./bin/cli/porcupine

ducklake-bench:
	CGO_ENABLED=1 go test ./internal/porcupine -run '^$$' -bench '^BenchmarkDuckLake' -benchtime=10x -benchmem -timeout=120s

ducklake-batching-bench:
	PORCUPINE_BENCH_BATCHING=1 sh scripts/analytics-benchmark.sh

ducklake-architecture-test:
	sh scripts/architecture-benchmark.sh

.PHONY: ducklake-s3-test
ducklake-s3-test:
	CGO_ENABLED=0 go run ./internal/cmd/ducklake-s3-integration
	CGO_ENABLED=0 go run ./internal/cmd/ducklake-s3-integration -tls

.PHONY: ducklake-restore-drill
ducklake-restore-drill: cli
	docker build --build-arg VERSION=dev -t porcupine-native-restore-test .
	PORCUPINE_RESTORE_DOCKER_IMAGE=porcupine-native-restore-test PORCUPINE_RESTORE_CLI="$(CURDIR)/bin/cli/porcupine" CGO_ENABLED=1 go test ./internal/porcupine -run '^TestDuckLakeCLIRestoreDrill$$' -count=1 -timeout=120s

.PHONY: shared-postgres-fixture-test shared-postgres-test
shared-postgres-fixture-test:
	CGO_ENABLED=0 PORCUPINE_TEST_NATIVE_POSTGRES=1 go test ./internal/cmd/shared-postgres-integration -count=1 -timeout=600s

shared-postgres-test: shared-postgres-fixture-test
	CGO_ENABLED=0 go run ./internal/cmd/shared-postgres-integration

SHARED_TEST_RACE ?= true

.PHONY: shared-integration
shared-integration: shared-postgres-fixture-test
	CGO_ENABLED=0 go run ./internal/cmd/shared-postgres-integration -- go run ./internal/cmd/ducklake-s3-integration -run '^(TestShared|TestDuckLakeS3SharedPostgres)' -race=$(SHARED_TEST_RACE) -timeout=600s
