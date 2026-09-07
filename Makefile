.DEFAULT_GOAL := help

BIN := $(CURDIR)/bin

export PATH := $(BIN):$(PATH)

GOLANGCI_LINT_VERSION := v2.12.2
TEMPORAL_CLI_VERSION := v1.8.2

OS := $(shell go env GOOS)
ARCH := $(shell go env GOARCH)

.PHONY: configure
configure: $(BIN)/golangci-lint $(BIN)/temporal ## Install pinned repository tools into bin

$(BIN)/golangci-lint:
	GOBIN=$(BIN) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

$(BIN)/temporal:
	mkdir -p $(BIN)
	curl -sSfL https://github.com/temporalio/cli/releases/download/$(TEMPORAL_CLI_VERSION)/temporal_cli_$(patsubst v%,%,$(TEMPORAL_CLI_VERSION))_$(OS)_$(ARCH).tar.gz \
		| tar -xz -C $(BIN) temporal

.PHONY: test
test: ## Run unit tests and the dev-server integration suite
	go test -short ./...
	TEMPORAL_CLI=$(BIN)/temporal go test ./integration/ -timeout 5m

.PHONY: test-unit
test-unit: ## Run unit tests only
	go test -short ./...

COVER_THRESHOLD := 80

.PHONY: cover
cover: ## Run all tests with coverage and enforce the threshold
	TEMPORAL_CLI=$(BIN)/temporal go test -coverprofile=coverage.out -coverpkg=./pkg/...,./internal/... ./... -timeout 5m
	@go tool cover -func=coverage.out | tail -1
	@go tool cover -func=coverage.out | tail -1 | \
		awk '{gsub("%","",$$3); if ($$3+0 < $(COVER_THRESHOLD)) { printf "coverage %s%% is below the $(COVER_THRESHOLD)%% threshold\n", $$3; exit 1 }}'

.PHONY: lint
lint: ## Run Go linters
	$(BIN)/golangci-lint run ./...

.PHONY: build
build: ## Build all packages and the example
	go build ./...

.PHONY: help
help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "%-18s %s\n", $$1, $$2}'

.PHONY: ver
ver: ## Cut a release: make ver v=X.Y.Z (creates + pushes the tag → release CI)
	@set -eu; \
	v="$(v)"; \
	if ! echo "$$v" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$$'; then echo "usage: make ver v=X.Y.Z" >&2; exit 1; fi; \
	if [ -n "$$(git status --porcelain)" ]; then echo "working tree is dirty — commit first" >&2; exit 1; fi; \
	git tag "v$$v" && git push origin "v$$v"; \
	echo "tagged v$$v — release workflow running"

.PHONY: bump
bump: ## Bump release tag: make bump TYPE=patch|minor|major (default patch)
	@set -eu; \
	type="$(or $(TYPE),patch)"; \
	case "$$type" in patch|minor|major) ;; *) echo "TYPE must be patch|minor|major" >&2; exit 1 ;; esac; \
	cur="$$(git describe --tags --abbrev=0 2>/dev/null | sed -E 's/^v//' || true)"; \
	echo "$$cur" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$$' || cur="0.0.0"; \
	major="$${cur%%.*}"; rest="$${cur#*.}"; minor="$${rest%%.*}"; patch="$${rest##*.}"; \
	case "$$type" in \
	  patch) new="$$major.$$minor.$$((patch+1))" ;; \
	  minor) new="$$major.$$((minor+1)).0" ;; \
	  major) new="$$((major+1)).0.0" ;; \
	esac; \
	echo "bump $$cur -> $$new"; \
	$(MAKE) ver v="$$new"
