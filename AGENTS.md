# AGENTS.md — temporal-entity

Entity Lifecycle Pattern library on top of the vanilla Temporal Go SDK.
A standalone product of the organization: it does not import graphene and
graphene does not import it — the relation is conceptual only (this repo
is where the SDK surface ideas are proven out).

## Before making changes

1. Read `README.md` — it describes the purpose, the API shape, and the
   package boundaries; a change that alters any of those updates README
   in the same commit.
2. `make configure` — pinned tool versions install into `bin/`, nothing
   goes into the system.
3. `make lint` and `make test` must be green before push: 0 issues,
   0 failures.

## Code rules

- Implementation language is Go; code, names, and comments are English.
- The pattern semantics come from Temporal's platform control-plane
  article; any behavioral change to the chassis comes with a check in
  `integration/`.
- The public surface has no code generation: commands/queries are user
  types implementing the `entity.Command[Res]` / `entity.Query[Res]`
  contracts.
- Registration errors accumulate and surface as one list at `Register` —
  library code never panics.
- Identifiers are the named types of `pkg/entity` (`ResourceID`,
  `RequestID`, ...): literals by cast, external input through `ParseX`.
- **This repository is English-only** (deliberate deviation from the org
  rule): README, this file, code, comments, and commit messages — no
  Russian anywhere. The library targets an external audience.
- Commits are Conventional Commits (`type(scope): summary`, imperative,
  lowercase, no trailing period, ≤72 chars), no `Co-Authored-By`.

## Package boundaries

- `pkg/entity` — vocabulary shared by both sides. Imports nothing from
  this repository.
- `pkg/entdefine` — the kind-defining side (the chassis workflow code).
- `pkg/entclient` — the caller's side. Imports `entdefine` only for the
  `Definition` type taken by `Bind`.
- `internal/wire` — the wire contract between the two sides; never
  exposed.
- `examples/` — consumers of the library; nothing moves from `pkg/` or
  `internal/` into examples, and there are no reverse imports.
- `integration/` — the end-to-end suite against a real dev server
  (`bin/temporal`, version pinned in the Makefile).

## Tools

`bin/` is populated only by `make configure`; versions are pinned in the
Makefile, `latest` is forbidden, and a version bump is its own explicit
commit.
