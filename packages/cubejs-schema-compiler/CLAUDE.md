# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Testing

### Dialect SQL templates

Don't cover a dialect's Jinja SQL templates (`sqlTemplates()` in `src/adapter/*Query.ts`) with
unit tests that match the rendered SQL. Asserting that a template renders the string it was
written to render proves nothing about whether the database accepts it. Prefer an integration
test that runs the query against the real database: `test/integration/<dialect>/`, run with
`yarn integration:<dialect>` (Docker-based).

If the dialect has no runner in `test/integration/`, write the test in
`packages/cubejs-testing-drivers` instead. Tests there must be cross-dialect: add the query to
the shared suite in `src/tests/testQueries.ts` (models in `fixtures/_schemas.json`) so it runs
against every driver, not as a one-off for a single database.

Add a unit test only when neither kind of integration test is possible and the behaviour is
important enough to guard.
