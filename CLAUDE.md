# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

Cube is a semantic layer for building data applications. This is a monorepo containing the complete Cube ecosystem including:
- Cube backend server and core components
- Client libraries for JavaScript/React/Vue/Angular
- Database drivers for various data sources
- Documentation site
- Rust components (CubeSQL, CubeStore)

## Development Commands

**Note: This project uses Yarn as the package manager.**

### Core Build Commands
```bash
# Build all packages
yarn build

# Run TypeScript compilation across all packages
yarn tsc

# Watch mode for TypeScript compilation
yarn tsc:watch

# Clean build artifacts
yarn clean

# Run linting across all packages
yarn lint

# Fix linting issues
yarn lint:fix

# Lint package.json files
yarn lint:npm
```

### Testing Commands
```bash
# Run tests (most packages have individual test commands)
yarn test

# Test individual packages
cd packages/cubejs-[package-name]
yarn test
```

### Documentation Development

**IMPORTANT: `/docs-mintlify` is the active documentation site. `/docs` is the legacy
docs site and is deprecated — do NOT add or edit content there.** When asked to write or
update documentation, work in `/docs-mintlify` unless the user explicitly says otherwise.

```bash
cd docs-mintlify
yarn dev    # Start the Mintlify dev server
```

- Content is authored as `.mdx` under topic directories (e.g. `admin/ai/`, `docs/explore-analyze/`).
- Frontmatter uses `title` and `description` keys.
- Navigation is registered in `docs-mintlify/docs.json` (pages must be added to the
  relevant `group` to appear in the sidebar).
- Use Mintlify components: `<Note>`, `<Warning>`, `<Info>`, `<Tip>`, `<Steps>`/`<Step>`,
  `<CardGroup>`/`<Card>`. Internal links are root-relative (e.g. `/admin/ai/rules`).
- Keep docs concise — most changes are small, surgical edits to existing pages, not new
  pages or walls of text. Prefer editing an existing page over creating a new one.
- See `docs-mintlify/CLAUDE.md` for full conventions.

## Architecture Overview

### Monorepo Structure
- **`/packages`**: All JavaScript/TypeScript packages managed by Lerna
  - Core packages: `cubejs-server-core`, `cubejs-schema-compiler`, `cubejs-query-orchestrator` 
  - Client libraries: `cubejs-client-core`, `cubejs-client-react`, etc.
  - Database drivers: `cubejs-postgres-driver`, `cubejs-bigquery-driver`, etc.
  - API layer: `cubejs-api-gateway`
- **`/rust`**: Rust components including CubeSQL (SQL interface) and CubeStore (distributed storage)
- **`/docs-mintlify`**: Mintlify documentation site — **the active docs site** (author docs here)
- **`/docs`**: Legacy Next.js/Nextra documentation site — **deprecated**, do not edit
- **`/examples`**: Example implementations and recipes

### Key Components
1. **Schema Compiler**: Compiles data models into executable queries
2. **Query Orchestrator**: Manages query execution, caching, and pre-aggregations
3. **API Gateway**: Provides REST, GraphQL, and SQL APIs
4. **CubeSQL**: Postgres-compatible SQL interface (Rust)
5. **CubeStore**: Distributed OLAP storage engine (Rust)
6. **Tesseract**: Native SQL planner (Rust) located in `/rust/cube/cubesqlplanner` - the default planner; set `CUBEJS_TESSERACT_SQL_PLANNER=false` to fall back to the deprecated legacy planner. Tesseract pre-aggregation planning follows this flag and cannot be toggled independently

### Package Management
- Uses Yarn workspaces with Lerna for package management
- TypeScript compilation is coordinated across packages
- Jest for unit testing with package-specific configurations

## Testing Approach

### Unit Tests
- Most packages have Jest-based unit tests in `/test` directories
- TypeScript packages use `jest.config.js` with TypeScript compilation
- Snapshot testing for SQL compilation and query planning

### Integration Tests
- Driver-specific integration tests in `/packages/cubejs-testing-drivers`
- End-to-end tests in `/packages/cubejs-testing`
- Docker-based testing environments for database drivers

### Test Commands
```bash
# Individual package testing
cd packages/[package-name]
yarn test

# Driver integration tests (requires Docker)
cd packages/cubejs-testing-drivers
yarn test
```

## Development Workflow

1. **Making Changes**: Work in individual packages, changes are coordinated via Lerna
2. **Building**: Use `yarn tsc` to compile TypeScript across all packages
3. **Testing**: Run relevant tests for modified packages
4. **Linting**: Ensure code passes `yarn lint` before committing

## Git

Use conventional commits with these prefixes:
- `feat:` — new features
- `fix:` — bug fixes
- `docs:` — documentation changes
- `refactor:` — code refactoring

Include scope in parentheses when applicable, e.g., `fix(tesseract):` or `feat(databricks-jdbc-driver):`.

## Common File Patterns

- `*.test.ts/js`: Jest unit tests
- `jest.config.js`: Jest configuration per package
- `tsconfig.json`: TypeScript configuration (inherits from root)
- `CHANGELOG.md`: Per-package changelogs maintained by Lerna
- `src/`: Source code directory
- `dist/`: Compiled output (not committed)

## Important Notes

- Documentation lives in `/docs-mintlify` (active, Mintlify). `/docs` is the legacy docs
  site and is deprecated — do not add or edit content there. See `docs-mintlify/CLAUDE.md`.
- The main Cube application development happens in `/packages`
- For data model changes, focus on `cubejs-schema-compiler` package
- For query execution changes, focus on `cubejs-query-orchestrator` package
- Database connectivity is handled by individual driver packages

## Key Dependencies

- **Lerna**: Monorepo management and publishing
- **TypeScript**: Primary language for most packages
- **Jest**: Testing framework
- **Rollup**: Bundling for client libraries
- **Docker**: Testing environments for database drivers