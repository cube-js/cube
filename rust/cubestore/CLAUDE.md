# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

CubeStore is the Rust-based distributed OLAP storage engine for Cube.js, designed to store and serve pre-aggregations at scale. It's part of the larger Cube.js monorepo and serves as the materialized cache store for rollup tables.

## Architecture Overview

### Core Components

The codebase is organized as a Rust workspace with multiple crates:

- **`cubestore`**: Main CubeStore implementation with distributed storage, query execution, and API interfaces
- **`cubestore-sql-tests`**: SQL compatibility test suite and benchmarks
- **`cubehll`**: HyperLogLog implementation for approximate distinct counting
- **`cubedatasketches`**: DataSketches integration for advanced approximate algorithms
- **`cubezetasketch`**: Theta Sketch implementation for set operations
- **`cuberpc`**: RPC layer for distributed communication
- **`cuberockstore`**: RocksDB wrapper and storage abstraction

### Key Modules in `cubestore/src/`

- **`metastore/`**: Metadata management, table schemas, partitioning, and distributed coordination
- **`queryplanner/`**: Query planning, optimization, and physical execution planning using DataFusion
- **`store/`**: Core storage layer with compaction and data management
- **`cluster/`**: Distributed cluster management, worker pools, and inter-node communication
- **`table/`**: Table data handling, Parquet integration, and data redistribution
- **`cachestore/`**: Caching layer with eviction policies and queue management
- **`sql/`**: SQL parsing and execution layer
- **`streaming/`**: Kafka streaming support and traffic handling
- **`remotefs/`**: Cloud storage integration (S3, GCS, MinIO)
- **`config/`**: Dependency injection and configuration management

## Development Commands

### Building

```bash
# Build all crates in release mode
cargo build --release

# Build all crates in debug mode
cargo build

# Build specific crate
cargo build -p cubestore

# Check code without building
cargo check
```

### Testing

```bash
# Run all tests
cargo test

# Run tests for specific crate
cargo test -p cubestore
cargo test -p cubestore-sql-tests

# Run single test
cargo test test_name

# Run tests with output
cargo test -- --nocapture

# Run integration tests
cargo test --test '*'

# Run benchmarks
cargo bench
```

### Development

```bash
# Format code
cargo fmt

# Check formatting
cargo fmt -- --check

# Run clippy lints
cargo clippy

# Run with debug logging
RUST_LOG=debug cargo run

# Run specific binary
cargo run --bin cubestore

# Watch for changes (requires cargo-watch)
cargo watch -x check -x test
```

### JavaScript Wrapper Commands

```bash
# Build TypeScript wrapper
npm run build

# Run JavaScript tests
npm test

# Lint JavaScript code
npm run lint

# Fix linting issues
npm run lint:fix
```

## Key Dependencies and Technologies

- **DataFusion**: Apache Arrow-based query engine (using Cube's fork)
- **Apache Arrow/Parquet**: Columnar data format and processing
- **RocksDB**: Embedded key-value store for metadata
- **Tokio**: Async runtime for concurrent operations
- **sqlparser-rs**: SQL parsing (using Cube's fork)

## Configuration via Dependency Injection

The codebase uses a custom dependency injection system defined in `config/injection.rs`. Services are configured through the `Injector` and use `Arc<dyn ServiceTrait>` patterns for abstraction.

## Testing Approach

- Unit tests are colocated with source files using `#[cfg(test)]` modules
- Integration tests are in `cubestore-sql-tests/tests/`
- SQL compatibility tests use fixtures in `cubestore-sql-tests/src/tests.rs`
- Benchmarks are in `benches/` directories

## Important Notes

- **Rust Nightly**: Uses nightly-2025-08-01 (see `rust-toolchain.toml`)
- Uses custom forks of Arrow/DataFusion and sqlparser-rs for Cube-specific features
- Distributed mode involves router and worker nodes communicating via RPC
- Heavy use of async/await patterns with Tokio runtime
- Parquet files are the primary storage format for data

## Docker Configuration

The project includes Docker configurations for building and deploying CubeStore:

- **`builder.Dockerfile`**: Defines the base build image with Rust nightly-2025-08-01, LLVM 18, and build dependencies
- **`Dockerfile`**: Production Dockerfile that uses `cubejs/rust-builder:bookworm-llvm-18` base image and copies rust-toolchain.toml
- **GitHub Actions**: Multiple CI/CD workflows use the same Rust version

## Updating Rust Version

When updating the Rust version, ensure ALL these files are kept in sync:

1. **`rust-toolchain.toml`** - Primary source of truth for local development
2. **`builder.Dockerfile`** - Update the rustup default command with the new nightly version
3. **`Dockerfile`** - Copies rust-toolchain.toml (no manual update needed if builder image is updated)
4. **GitHub Workflows** - Update all occurrences of the Rust nightly version in `.github/workflows/` directory

**Note**: The `cubejs/rust-builder:bookworm-llvm-18` Docker image tag may also need updating if the builder.Dockerfile changes significantly.
