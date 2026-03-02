# AGENTS.md

## Cursor Cloud specific instructions

### Overview

Cube is a semantic layer monorepo. See `CLAUDE.md` for full architecture and command reference. Key packages live in `/packages`, Rust components in `/rust`.

### Building

- `yarn install` installs all workspace dependencies.
- `yarn tsc` compiles all TypeScript packages (incremental build). Use `yarn clean && yarn tsc` if you hit stale cache issues.
- `yarn build` builds client library bundles (rollup). Required before building the playground.
- To build the playground UI: `cd packages/cubejs-playground && yarn build:playground`. This copies output to `packages/cubejs-server-core/playground/`.

### Running the dev server

Create a working directory outside the repo (e.g. `/tmp/cube-test-project`) with a `model/cubes/` folder containing your cube definitions. Then start:

```bash
cd /tmp/cube-test-project
CUBEJS_DEV_MODE=true CUBEJS_DB_TYPE=postgres CUBEJS_API_SECRET=test123 \
  node /workspace/packages/cubejs-server/bin/dev-server
```

The server listens on port 4000 (API + Playground), CubeStore on 3030, and Postgres-compatible SQL on 15432. No external database is needed to verify the server starts and the Playground loads; queries will fail without a real DB, but model loading and the API meta endpoint work.

### Linting & Testing

- `yarn lint:npm` — lint package.json files (fast).
- `yarn lint` — full lint across all packages (runs per-package ESLint; see `CLAUDE.md`).
- Individual package tests: `cd packages/cubejs-<name> && yarn jest` (some use `yarn unit` or `yarn test`; check each package's `package.json` scripts).
- Schema compiler unit tests: `cd packages/cubejs-schema-compiler && yarn unit`.
- Some snapshot tests may show formatting diffs on newer Node.js versions; these are pre-existing and not caused by local changes.

### Gotchas

- The root `yarn tsc` uses project references and `composite: true`. All packages must compile cleanly for the build to succeed.
- The playground must be built separately (`yarn build:playground` in `packages/cubejs-playground`) before the dev server will serve the Playground UI. Without it, the API still works.
- `@cubejs-backend/native` provides optional Rust bindings (CubeSQL/Tesseract). Pre-built binaries are downloaded at install time. If unavailable for your platform, the SQL interface degrades gracefully.
