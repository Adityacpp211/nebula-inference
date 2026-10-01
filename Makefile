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
CMD_nebula-gateway      := ./services/gateway
CMD_nebula-migrate      := ./cmd/nebula-migrate
BINARIES := nebula-controlplane nebula-gateway nebula-migrate

# ---- local development database --------------------------------------------
# Development credentials. Deliberately NOT valid in production: config
# validation refuses well-known passwords when NEBULA_ENV=production.
# An already-exported NEBULA_DATABASE_URL / NEBULA_TEST_DATABASE_URL wins, so
# these targets work against a database that is not the compose one.
DEV_DB_URL  ?= $(or $(NEBULA_DATABASE_URL),postgres://nebula:nebula@127.0.0.1:5432/nebula?sslmode=disable)
TEST_DB_URL ?= $(or $(NEBULA_TEST_DATABASE_URL),postgres://nebula:nebula@127.0.0.1:5432/nebula_test?sslmode=disable)

# A fixed development pepper, so restarting the control plane does not invalidate
# the seeded API key you just copied out of the log. Config validation refuses
# this exact value when NEBULA_ENV=production, which is what keeps it honest.
DEV_KEY_PEPPER ?= $(or $(NEBULA_AUTH_KEY_PEPPER),nebula-development-pepper-do-not-use-in-production)

# The secret the gateway signs X-Nebula-Auth-Context with and the control plane
# verifies (docs/security-boundaries.md §2, B2). Refused by name in production.
DEV_INTERNAL_SECRET ?= $(or $(NEBULA_INTERNAL_AUTH_SECRET),nebula-development-internal-secret-do-not-use)
DEV_REDIS_URL       ?= $(or $(NEBULA_REDIS_URL),redis://127.0.0.1:6379/0)

# In a restricted network where proxy.golang.org is unreachable but github.com is
# not, export GOPROXY=direct GOSUMDB=off before running these targets.

# ---- inference worker (Python) ---------------------------------------------
# The worker is the one component that is not Go (ADR-0002: Python only where the
# model libraries require it). It gets its own targets rather than being folded into
# `test`, because its integration suite needs an engine binary and a model file that
# a Go developer has no reason to have.
WORKER_DIR := workers/inference
PY         ?= python3

# The engine binary and the fixture model. Both are paths, never defaults that might
# exist: a test that silently runs against the wrong model is worse than one that
# refuses to run. `make worker-model` produces the second.
WORKER_ENGINE_BIN  ?= $(NEBULA_LLAMA_SERVER_BIN)
WORKER_MODEL_PATH  ?= $(or $(NEBULA_TEST_MODEL_PATH),$(CURDIR)/.cache/nebula-tiny.gguf)
WORKER_MODEL_VER   ?= nebula-tiny:fixture

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
##
## Two packages, not one: tests/integration covers the schema, and
## services/controlplane/tests covers the API and store. The second cannot live
## beside the first because Go's internal-package rule keeps the control plane's
## handlers out of reach from the repository root.
.PHONY: test-integration
test-integration:
	@NEBULA_TEST_DATABASE_URL="$(TEST_DB_URL)" go test -race -count=1 \
		./tests/integration/... ./services/controlplane/tests/...

## worker-install: install the inference worker with its development extra
.PHONY: worker-install
worker-install:
	@cd $(WORKER_DIR) && $(PY) -m pip install -e '.[dev]'

## worker-lint: ruff and mypy --strict over the inference worker
.PHONY: worker-lint
worker-lint:
	@cd $(WORKER_DIR) && $(PY) -m ruff check . && $(PY) -m ruff format --check . \
		&& $(PY) -m mypy --strict nebula_worker

## worker-fmt: format the inference worker
.PHONY: worker-fmt
worker-fmt:
	@cd $(WORKER_DIR) && $(PY) -m ruff format . && $(PY) -m ruff check --fix .

## worker-test: worker tests that need no engine and no model
.PHONY: worker-test
worker-test:
	@cd $(WORKER_DIR) && $(PY) -m pytest -q -m 'not integration'

## worker-model: train the tiny fixture model used by the worker integration tests
##
## Trained rather than downloaded (ADR-0028). Needs the dev extra for torch and gguf,
## and the engine's own llama-tokenize so the model trains against exactly the
## tokenizer it will be served with.
.PHONY: worker-model
worker-model:
	@test -n "$(WORKER_ENGINE_BIN)" || { \
		echo "set NEBULA_LLAMA_SERVER_BIN to a llama-server binary"; exit 1; }
	@mkdir -p $(dir $(WORKER_MODEL_PATH))
	@cd $(WORKER_DIR) && $(PY) tools/make_tiny_model.py \
		--llama-tokenize "$(dir $(WORKER_ENGINE_BIN))llama-tokenize" \
		--out "$(WORKER_MODEL_PATH)"
	@echo "wrote $(WORKER_MODEL_PATH)"

## worker-test-integration: worker tests against the real engine and a real model
##
## Requires NEBULA_LLAMA_SERVER_BIN and a fixture model (see worker-model). Refuses to
## run rather than skipping silently: a green run that quietly tested nothing is the
## failure mode this suite exists to prevent.
.PHONY: worker-test-integration
worker-test-integration:
	@test -n "$(WORKER_ENGINE_BIN)" || { \
		echo "set NEBULA_LLAMA_SERVER_BIN to a llama-server binary"; exit 1; }
	@test -f "$(WORKER_MODEL_PATH)" || { \
		echo "no model at $(WORKER_MODEL_PATH) — run 'make worker-model'"; exit 1; }
	@cd $(WORKER_DIR) && \
		NEBULA_LLAMA_SERVER_BIN="$(WORKER_ENGINE_BIN)" \
		NEBULA_TEST_MODEL_PATH="$(WORKER_MODEL_PATH)" \
		NEBULA_TEST_MODEL_VERSION="$(WORKER_MODEL_VER)" \
		$(PY) -m pytest -q

## run-worker-mock: run the inference worker with the declared stub runtime
##
## No engine, no model, no real tokens. Useful for exercising the worker API, the
## admission queue and the probes; useless for anything about output.
.PHONY: run-worker-mock
run-worker-mock:
	@cd $(WORKER_DIR) && NEBULA_ENV=dev NEBULA_LOG_LEVEL=debug \
		NEBULA_WORKER_RUNTIME=mock NEBULA_WORKER_MODEL_VERSION=dev:mock \
		$(PY) -m nebula_worker.main

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

## deps-up: start PostgreSQL and Redis
.PHONY: deps-up
deps-up: db-up
	@docker compose up -d redis
	@for i in $$(seq 1 30); do \
		if docker compose exec -T redis redis-cli ping >/dev/null 2>&1; then echo "  redis ready"; exit 0; fi; \
		sleep 1; \
	done; echo "redis did not become ready in 30s"; exit 1

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
##
## Seeds a development organization and a full-scope API key on first run and
## prints the key once. The pepper is fixed here so a restart does not invalidate
## the key you just copied; config validation refuses this value when
## NEBULA_ENV=production.
.PHONY: run-controlplane
run-controlplane:
	@NEBULA_DATABASE_URL="$(DEV_DB_URL)" NEBULA_LOG_FORMAT=text NEBULA_ENV=dev \
		NEBULA_AUTH_KEY_PEPPER="$(DEV_KEY_PEPPER)" \
		NEBULA_INTERNAL_AUTH_SECRET="$(DEV_INTERNAL_SECRET)" \
		NEBULA_DEV_SEED=true NEBULA_DEV_MOCK_RUNTIME=true \
		go run ./services/controlplane

## run-gateway: run the gateway on :8080 against the local control plane and Redis
##
## Routes come from deploy/dev/routes.yaml (static routing, Phase 4), which points
## the seeded "dev" organization's model "nebula-mock" at `make run-worker-mock`.
.PHONY: run-gateway
run-gateway:
	@NEBULA_LOG_FORMAT=text NEBULA_ENV=dev \
		NEBULA_AUTH_KEY_PEPPER="$(DEV_KEY_PEPPER)" \
		NEBULA_INTERNAL_AUTH_SECRET="$(DEV_INTERNAL_SECRET)" \
		NEBULA_REDIS_URL="$(DEV_REDIS_URL)" \
		NEBULA_GATEWAY_ROUTES_FILE=deploy/dev/routes.yaml \
		go run ./services/gateway

## e2e-gateway: the Phase 4 demo — the OpenAI Python SDK against a live stack
##
## Starts PostgreSQL and Redis, migrates a throwaway database, runs the control
## plane (seeded), a mock worker and the gateway, then runs tests/e2e/gateway with
## the unmodified OpenAI SDK. Everything it starts, it stops.
.PHONY: e2e-gateway
e2e-gateway: build
	@./scripts/e2e-gateway.sh

## dev-up: NEBULA on a local kind cluster — images built and loaded, chart installed
.PHONY: dev-up
dev-up:
	@./scripts/dev-up.sh

## dev-down: delete the kind cluster (the host model cache is kept)
.PHONY: dev-down
dev-down:
	@./scripts/dev-down.sh

## e2e-kind: the Phase 5 demo against the kind cluster dev-up created
.PHONY: e2e-kind
e2e-kind:
	@$(PY) tests/e2e/kind/phase5_demo.py

## helm-check: lint the chart and render it with dev and production-shaped values
.PHONY: helm-check
helm-check:
	@helm lint deploy/helm/nebula -f deploy/helm/nebula/values-dev.yaml
	@helm template nebula deploy/helm/nebula -n nebula-system -f deploy/helm/nebula/values-dev.yaml >/dev/null
	@helm template nebula deploy/helm/nebula -n nebula-system --set images.tag=ci \
		--set secrets.keyPepper=x --set secrets.internalAuthSecret=x --set secrets.databaseURL=x \
		--set secrets.controllerDatabaseURL=x --set secrets.redisURL=x --set secrets.artifactAccessKey=x \
		--set secrets.artifactSecretKey=x --set artifact.endpoint=s3.example:443 >/dev/null
	@echo "chart lints and renders for dev and production values"

## load-gateway: the Phase 4 load baseline (k6) against the same live stack
.PHONY: load-gateway
load-gateway: build
	@./scripts/load-gateway.sh

## docker-build: build the container images
.PHONY: docker-build
docker-build:
	@docker build -f deploy/docker/Dockerfile.controlplane \
		--build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) --build-arg BUILD_TIME=$(BUILD_TIME) \
		-t nebula/controlplane:$(VERSION) .
	@docker build -f deploy/docker/Dockerfile.gateway \
		--build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) --build-arg BUILD_TIME=$(BUILD_TIME) \
		-t nebula/gateway:$(VERSION) .
	@docker build -f deploy/docker/Dockerfile.migrate \
		--build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) --build-arg BUILD_TIME=$(BUILD_TIME) \
		-t nebula/migrate:$(VERSION) .
	@docker build -f deploy/docker/Dockerfile.worker --target mock \
		--build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) --build-arg BUILD_TIME=$(BUILD_TIME) \
		-t nebula/worker-mock:$(VERSION) .
	@docker build -f deploy/docker/Dockerfile.worker --target llamacpp \
		--build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) --build-arg BUILD_TIME=$(BUILD_TIME) \
		-t nebula/worker-llamacpp:$(VERSION) .

## tidy: tidy go.mod and go.sum
.PHONY: tidy
tidy:
	@go mod tidy
	@go mod verify

## verify-phase: the phase gate — everything that must be green to finish a phase
.PHONY: verify-phase
verify-phase: fmt-check vet build test worker-lint worker-test
	@echo
	@echo "Go and worker unit tests, build, formatting and worker typing are green."
	@echo "run 'make test-integration' with a database and"
	@echo "'make worker-test-integration' with an engine binary to complete the gate."

## clean: remove build output
.PHONY: clean
clean:
	@rm -rf $(BIN_DIR) coverage.out coverage.html
	@rm -rf $(WORKER_DIR)/.pytest_cache $(WORKER_DIR)/.mypy_cache $(WORKER_DIR)/.ruff_cache
	@find $(WORKER_DIR) -name __pycache__ -type d -prune -exec rm -rf {} +
