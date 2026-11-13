.PHONY: build run test check fmt integration integration-s3

build:
	CGO_ENABLED=0 go build -trimpath -o bin/porcupine ./cmd/porcupine

run:
	go run ./cmd/porcupine

test:
	go test ./...

check: cli-check
	go vet ./...
	go test ./...
	@test -z "$$(gofmt -l cmd internal)" || (echo 'Run make fmt'; exit 1)

fmt:
	gofmt -w cmd internal cli/cmd

integration:
	python3 scripts/integration.py

integration-s3:
	python3 scripts/integration.py --s3

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

# Run the same symbol-level vulnerability scan and bounded parser fuzzing as CI.
security-check:
	go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...
	go test ./internal/porcupine -run '^$$' -fuzz '^FuzzValidateSQL$$' -fuzztime=10s -parallel=2
	go test ./internal/porcupine -run '^$$' -fuzz '^FuzzCaptureBatch$$' -fuzztime=10s -parallel=2

restore-drill:
	python3 scripts/restore_drill.py

.PHONY: online-restore-drill
online-restore-drill:
	python3 scripts/online_restore_drill.py
	python3 scripts/default_restore_drill.py

.PHONY: cli cli-check cli-generate
cli:
	cd cli && CGO_ENABLED=0 go build -trimpath -o ../bin/cli/porcupine ./cmd/porcupine

cli-generate:
	go run ./internal/cmd/gencli

cli-check:
	go run ./internal/cmd/gencli -check
	cd cli && go vet ./... && go test ./...
	@test -z "$$(gofmt -l cli/cmd)" || (echo 'Run make fmt'; exit 1)

.PHONY: docs
docs:
	python3 scripts/build_docs.py

.PHONY: docs-preview
docs-preview: docs
	python3 scripts/serve_docs.py

.PHONY: ducklake-build ducklake-test ducklake-smoke ducklake-bench ducklake-compare
ducklake-build:
	CGO_ENABLED=1 go build -tags ducklake -trimpath -o bin/porcupine-ducklake ./cmd/porcupine

.PHONY: ducklake-performance-compare
ducklake-performance-compare:
	sh scripts/analytics-benchmark.sh

.PHONY: ducklake-batching-bench
ducklake-batching-bench:
	PORCUPINE_BENCH_BATCHING=1 sh scripts/analytics-benchmark.sh

ducklake-test:
	CGO_ENABLED=1 go test -race -tags ducklake ./internal/porcupine -run '^TestDuckLake' -count=1 -timeout=120s

ducklake-smoke: ducklake-build cli
	go run ./internal/cmd/ducklake-smoke -server ./bin/porcupine-ducklake -cli ./bin/cli/porcupine

ducklake-bench:
	CGO_ENABLED=1 go test -tags ducklake ./internal/porcupine -run '^$$' -bench '^BenchmarkDuckLake' -benchtime=10x -benchmem -timeout=120s

ducklake-compare:
	CGO_ENABLED=1 GOFLAGS=-tags=ducklake python3 scripts/integration.py
