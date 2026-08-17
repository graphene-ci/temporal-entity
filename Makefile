.DEFAULT_GOAL := help

BIN := $(CURDIR)/bin

export PATH := $(BIN):$(PATH)

GOLANGCI_LINT_VERSION := v2.12.2
TEMPORAL_CLI_VERSION := v1.8.2

OS := $(shell go env GOOS)
ARCH := $(shell go env GOARCH)

.PHONY: configure
configure: ## Install pinned repository tools into bin
	GOBIN=$(BIN) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	curl -sSfL https://github.com/temporalio/cli/releases/download/$(TEMPORAL_CLI_VERSION)/temporal_cli_$(patsubst v%,%,$(TEMPORAL_CLI_VERSION))_$(OS)_$(ARCH).tar.gz \
		| tar -xz -C $(BIN) temporal

.PHONY: test
test: ## Run unit tests and the dev-server integration suite
	go test -short ./...
	TEMPORAL_CLI=$(BIN)/temporal go test ./integration/ -timeout 5m

.PHONY: test-unit
test-unit: ## Run unit tests only
	go test -short ./...

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
