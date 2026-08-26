# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

CubeSQL is a SQL proxy server that enables SQL-based access to Cube.js semantic layer. It emulates the PostgreSQL wire protocol, allowing standard SQL clients and BI tools to query Cube.js deployments as if they were traditional databases. Note: MySQL protocol support has been deprecated and is no longer available.

This is a Rust workspace containing three crates:
- **cubesql**: Main SQL proxy server with query compilation and protocol emulation
- **cubeclient**: Rust client library for Cube.js API communication
- **pg-srv**: PostgreSQL wire protocol server implementation

## Development Commands

### Prerequisites
```bash
# Install required Rust toolchain (1.90.0)
rustup update

# Install snapshot testing tool
cargo install cargo-insta
```

### Core Build Commands
```bash
# Build all workspace members
cargo build

# Build release version
cargo build --release

# Format code
cargo fmt

# Run linting (note: many clippy rules are disabled)
cargo clippy
```

### Running CubeSQL Server
```bash
# Run with required environment variables
CUBESQL_CUBE_URL=$CUBE_URL/cubejs-api \
CUBESQL_CUBE_TOKEN=$CUBE_TOKEN \
CUBESQL_LOG_LEVEL=debug \
CUBESQL_BIND_ADDR=0.0.0.0:4444 \
cargo run --bin cubesqld

# Connect via PostgreSQL client
psql -h 127.0.0.1 -p 4444 -U root
```

### Testing Commands
```bash
# Run all unit tests
cargo test

# Run specific test module
cargo test test_introspection
cargo test test_udfs

# Run integration tests (requires Cube.js instance)
cargo test --test e2e

# Review snapshot test changes
cargo insta review

# Run benchmarks
cargo bench
```

## Architecture Overview

### Query Processing Pipeline
1. **Protocol Layer**: Accepts PostgreSQL wire protocol connections
2. **SQL Parser**: Modified sqlparser-rs parses incoming SQL queries
3. **Query Rewriter**: egg-based rewrite engine transforms SQL to Cube.js queries
4. **Compilation**: Generates Cube.js REST API calls or DataFusion execution plans
5. **Execution**: DataFusion executes queries or proxies to Cube.js
6. **Result Formatting**: Converts results back to wire protocol format

### Key Components

#### cubesql crate structure:
- **`/compile`**: SQL compilation and query planning
  - `/engine`: DataFusion integration and query execution
  - `/rewrite`: egg-based query optimization rules
- **`/sql`**: Database protocol implementations
  - `/postgres`: PostgreSQL system catalog emulation
  - `/database_variables`: Variable system for PostgreSQL protocol
- **`/transport`**: Network transport and session management
- **`/config`**: Configuration and service initialization

#### Testing Approach:
- **Unit Tests**: Inline tests in source files using `#[cfg(test)]`
- **Integration Tests**: End-to-end tests in `/e2e` directory
- **Snapshot Tests**: Extensive use of `insta` for SQL compilation snapshots
- **BI Tool Tests**: Compatibility tests for Metabase, Tableau, PowerBI, etc.

### Important Implementation Details

1. **DataFusion Integration**: Uses forked Apache Arrow DataFusion for query execution
2. **Rewrite Rules**: Complex SQL transformations using egg e-graph library
3. **Protocol Emulation**: Implements enough of PostgreSQL protocol for BI tools
4. **System Catalogs**: Emulates pg_catalog (PostgreSQL)
5. **Variable Handling**: Supports SET/SHOW commands for protocol compatibility

## Common Development Tasks

### Adding New SQL Support
1. Add parsing support in `/compile/parser`
2. Create rewrite rules in `/compile/rewrite/rules`
3. Add tests with snapshot expectations
4. Update protocol-specific handling if needed

### Rewrite rules: never traverse a list recursively

Every matcher must match exactly **one** level. A list is consumed by dedicated rules
that match the list node itself — never by a transform that walks it.

- **Lists are flat, not head/tail.** A list node holds all of its elements as children
  (`UnionInputs(a, b, c)`), not nested cons cells (`UnionInputs(a, UnionInputs(b, ...))`).
  Cons lists are legacy — do not add new ones, and prefer converting one you touch.
  Build them with the flat branch of `add_expr_flat_list_node!` (or an equivalent that
  adds a single node with every element), register the node in `ListType`, and traverse
  with `flat_list_pushdown_pullup_rules` / `replacer_flat_push_down_node` /
  `replacer_flat_pull_up_node`, or `transforming_list_rewrite_with_lists_and_vars` when the
  output needs a node no pattern can spell out (a cleared replacer context, an alias
  converted to another node type). The flat pull-up matches the whole list in one rule and
  takes `top_level_elem_vars`, which is how a fact that must hold across every element —
  all queries reaching the same data source — is enforced: name the variable there and
  unification does the rest, with no comparison of your own.
- Generate the traversal with the existing helpers rather than by hand:
  `WrapperRules::list_pushdown_pullup_rules` / `flat_list_pushdown_pullup_rules`
  (or `replacer_push_down_node` / `replacer_pull_up_node` underneath them). They emit
  the `-push-down`, `-pull-up` and `-tail` rules that distribute a replacer over the
  list's elements and collect it back once they are all done.
- Push-down and pull-up are **separate rules**. Do not fold both directions, or the
  list walk, into one rewrite with a big transform.
- Do not write an imperative reader over `egraph[id].nodes` to collect a list's
  elements inside a transform. An e-class holds many representations, so picking one is
  arbitrary; the cons shape is also an implementation detail of how the list was built
  (`add_plan_list_node!`), which a hand-rolled reader silently couples itself to.
- A transform should only decide scalar facts (a flag, an alias, whether a template
  exists) about nodes the **pattern** already bound. Relationships between several
  matched nodes — "both sides reach the same data source" — belong in the pattern, by
  reusing one pattern variable in both places, so unification enforces them.
- A push-down replacer must never end up on top of an already pulled-up subtree. Inputs
  that arrive as a finished `cube_scan_wrapper(wrapper_pullup_replacer(..))` — the queries
  of a set operation, the sides of a join — have nothing left to push into, so their list
  carries a **pull-up** replacer and is consumed by pull-up rules. Putting a push-down
  replacer there instead makes `wrapper-subqueries-wrapped-scan-to-pull`
  (`rules/wrapper/subquery.rs`) match, which re-contexts the element without comparing
  input data sources — see the `TODO` on it. That rule is for the subquery path only;
  reaching it from anywhere else means the rules above it are shaped wrong.

### Debugging Query Compilation
```bash
# Enable detailed logging
CUBESQL_LOG_LEVEL=trace cargo run --bin cubesqld

# Check rewrite traces in logs
# Look for "Rewrite" entries showing transformation steps
```

### Working with Snapshots
```bash
# After making changes that affect SQL compilation
cargo test
cargo insta review  # Review and accept/reject changes
```

## Key Dependencies

- **DataFusion**: Query execution engine (forked version with custom modifications)
- **sqlparser-rs**: SQL parser (forked with CubeSQL-specific extensions)
- **egg**: E-graph library for query optimization
- **tokio**: Async runtime for network and I/O operations
- **pgwire**: PostgreSQL wire protocol implementation

## Important Notes

- This codebase uses heavily modified forks of DataFusion and sqlparser-rs
- Many clippy lints are disabled due to code generation and complex patterns
- Integration tests require a running Cube.js instance
- The rewrite engine is performance-critical and uses advanced optimization techniques
- Protocol compatibility is paramount for BI tool support