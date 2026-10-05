# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Testing

### Dialect SQL templates

Don't cover a dialect's Jinja SQL templates (`sqlTemplates()` in `src/adapter/*Query.ts`) with
unit tests that match the rendered SQL. Asserting that a template renders the string it was
written to render proves nothing about whether the database accepts it. Prefer an integration
test that runs the query against the real database: `test/integration/<dialect>/`, run with
`yarn integration:<dialect>` (Docker-based).

Add a unit test only when an integration test is not possible (no Docker image or runner for
the dialect) and the behaviour is important enough to guard.
