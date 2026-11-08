.PHONY: build run test check fmt integration integration-s3

build:
	CGO_ENABLED=0 go build -trimpath -o bin/porcupine ./cmd/porcupine

run:
	go run ./cmd/porcupine

test:
	go test ./...

check:
	go vet ./...
	go test ./...
	@test -z "$$(gofmt -l cmd internal)" || (echo 'Run make fmt'; exit 1)

fmt:
	gofmt -w cmd internal

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
