# NEBULA — the single entry point for every task.
#
# CI and a human run exactly the same commands. If a target here does not exist,
# the task is not automated, and that is a bug rather than a convention.
#
# Phase 1 provides the targets Phase 1 can honestly implement. Targets for later
# phases are added by the phase that makes them real, rather than existing as
# stubs that pretend to work.

SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

# ---- build metadata --------------------------------------------------------
VERSION   ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT    ?= $(shell git rev-parse HEAD 2>/dev/null || echo unknown)
BUILD_TIME ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

MODULE   := github.com/adityasatwar321/nebula
PKG_VER  := $(MODULE)/packages/version
LDFLAGS  := -s -w \
	-X $(PKG_VER).Version=$(VERSION) \
	-X $(PKG_VER).Commit=$(COMMIT) \
	-X $(PKG_VER).BuildTime=$(BUILD_TIME)

BIN_DIR := bin

# Binary name -> source package. Explicit rather than derived: the service
# directory is named for the component (controlplane) while the binary carries
# the product prefix (nebula-controlplane), and guessing that mapping is how a
# build target silently stops covering a new service.
CMD_nebula-controlplane := ./services/controlplane
CMD_nebula-migrate      := ./cmd/nebula-migrate
BINARIES := nebula-controlplane nebula-migrate

# ---- local development database --------------------------------------------
# Development credentials. Deliberately NOT valid in production: config
# validation refuses well-known passwords when NEBULA_ENV=production.
# An already-exported NEBULA_DATABASE_URL / NEBULA_TEST_DATABASE_URL wins, so
# these targets work against a database that is not the compose one.
DEV_DB_URL  ?= $(or $(NEBULA_DATABASE_URL),postgres://nebula:nebula@127.0.0.1:5432/nebula?sslmode=disable)
TEST_DB_URL ?= $(or $(NEBULA_TEST_DATABASE_URL),postgres://nebula:nebula@127.0.0.1:5432/nebula_test?sslmode=disable)

# In a restricted network where proxy.golang.org is unreachable but github.com is
# not, export GOPROXY=direct GOSUMDB=off before running these targets.

## help: list every target
.PHONY: help
help:
	@echo "NEBULA — available targets:"
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/^## /  /' | sort

## preflight: check that the required tools are installed
.PHONY: preflight
preflight:
	@./scripts/preflight.sh

## build: compile every binary into bin/
.PHONY: build
build:
	@mkdir -p $(BIN_DIR)
	@$(foreach b,$(BINARIES), \
		echo "  building $(b) from $(CMD_$(b))"; \
		CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/$(b) $(CMD_$(b)); \
	)

## fmt: format all Go code
.PHONY: fmt
fmt:
	@gofmt -w -s .

## fmt-check: fail if any file is not formatted
.PHONY: fmt-check
fmt-check:
	@unformatted=$$(gofmt -l -s .); \
	if [ -n "$$unformatted" ]; then \
		echo "these files are not formatted; run 'make fmt':"; echo "$$unformatted"; exit 1; \
	fi

## vet: run go vet
.PHONY: vet
vet:
	@go vet ./...

## lint: run golangci-lint if installed, otherwise fmt-check and vet
.PHONY: lint
lint: fmt-check vet
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not installed; ran gofmt and go vet only."; \
		echo "install: https://golangci-lint.run/welcome/install/ (CI always runs the full linter)"; \
	fi

## test: unit tests with the race detector and coverage
.PHONY: test
test:
	@go test -race -covermode=atomic -coverprofile=coverage.out ./packages/... ./services/... ./cmd/... ./migrations/...
	@go tool cover -func=coverage.out | tail -1

## test-short: unit tests without the race detector, for a fast inner loop
.PHONY: test-short
test-short:
	@go test ./packages/... ./services/... ./cmd/...

## test-integration: tests that need a real PostgreSQL (see NEBULA_TEST_DATABASE_URL)
.PHONY: test-integration
test-integration:
	@NEBULA_TEST_DATABASE_URL="$(TEST_DB_URL)" go test -race -count=1 ./tests/integration/...

## cover: open the HTML coverage report
.PHONY: cover
cover: test
	@go tool cover -html=coverage.out -o coverage.html
	@echo "wrote coverage.html"

## db-up: start the development PostgreSQL container
.PHONY: db-up
db-up:
	@docker compose up -d postgres
	@echo "waiting for postgres..."
	@for i in $$(seq 1 30); do \
		if docker compose exec -T postgres pg_isready -U nebula -q; then echo "  ready"; exit 0; fi; \
		sleep 1; \
	done; \
	echo "postgres did not become ready in 30s"; docker compose logs postgres; exit 1

## db-down: stop the development database and delete its data
.PHONY: db-down
db-down:
	@docker compose down -v

## migrate-up: apply every pending migration to the development database
.PHONY: migrate-up
migrate-up: build
	@NEBULA_DATABASE_URL="$(DEV_DB_URL)" $(BIN_DIR)/nebula-migrate up

## migrate-down: revert the last migration (override with N=3)
.PHONY: migrate-down
migrate-down: build
	@NEBULA_DATABASE_URL="$(DEV_DB_URL)" $(BIN_DIR)/nebula-migrate down $(or $(N),1)

## migrate-status: show which migrations are applied
.PHONY: migrate-status
migrate-status: build
	@NEBULA_DATABASE_URL="$(DEV_DB_URL)" $(BIN_DIR)/nebula-migrate status

## migrate-cycle: up, down to empty, then up again — the Phase 1 database gate
.PHONY: migrate-cycle
migrate-cycle: build
	@echo "== up =="
	@NEBULA_DATABASE_URL="$(DEV_DB_URL)" $(BIN_DIR)/nebula-migrate up
	@echo "== down to empty =="
	@NEBULA_DATABASE_URL="$(DEV_DB_URL)" $(BIN_DIR)/nebula-migrate down 99
	@echo "== up again =="
	@NEBULA_DATABASE_URL="$(DEV_DB_URL)" $(BIN_DIR)/nebula-migrate up
	@NEBULA_DATABASE_URL="$(DEV_DB_URL)" $(BIN_DIR)/nebula-migrate status

## run-controlplane: run the control plane against the development database
.PHONY: run-controlplane
run-controlplane:
	@NEBULA_DATABASE_URL="$(DEV_DB_URL)" NEBULA_LOG_FORMAT=text NEBULA_ENV=dev \
		go run ./services/controlplane

## docker-build: build the container images
.PHONY: docker-build
docker-build:
	@docker build -f deploy/docker/Dockerfile.controlplane \
		--build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) --build-arg BUILD_TIME=$(BUILD_TIME) \
		-t nebula/controlplane:$(VERSION) .
	@docker build -f deploy/docker/Dockerfile.migrate \
		--build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) --build-arg BUILD_TIME=$(BUILD_TIME) \
		-t nebula/migrate:$(VERSION) .

## tidy: tidy go.mod and go.sum
.PHONY: tidy
tidy:
	@go mod tidy
	@go mod verify

## verify-phase: the phase gate — everything that must be green to finish a phase
.PHONY: verify-phase
verify-phase: fmt-check vet build test
	@echo
	@echo "unit tests, build and formatting are green."
	@echo "run 'make test-integration' with a database to complete the gate."

## clean: remove build output
.PHONY: clean
clean:
	@rm -rf $(BIN_DIR) coverage.out coverage.html
