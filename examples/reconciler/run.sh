#!/usr/bin/env bash
# Demo runner: needs a running Temporal dev server on localhost:7233
# (e.g. `temporal server start-dev`), plus the EntityKind/EntityPhase
# search attributes if you enable them in the kind options:
#   temporal operator search-attribute create --name EntityKind --type Keyword
#   temporal operator search-attribute create --name EntityPhase --type Keyword
set -euo pipefail
cd "$(dirname "$0")"
go run .
