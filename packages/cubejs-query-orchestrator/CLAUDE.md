# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Package Overview

The Query Orchestrator is a multi-stage querying engine that manages query execution, caching, and pre-aggregations in Cube. It receives pre-aggregation SQL queries and executes them in exact order, ensuring up-to-date data structure and freshness.

## Development Commands

**Note: This project uses Yarn as the package manager.**

```bash
# Build the package
yarn build

# Build with TypeScript compilation
yarn tsc

# Watch mode for development
yarn watch

# Run all tests (unit + integration)
yarn test

# Run only unit tests
yarn unit

# Run only integration tests
yarn integration

# Run CubeStore integration tests specifically
yarn integration:cubestore

# Run linting
yarn lint

# Fix linting issues
yarn lint:fix
```

## Architecture Overview

### Core Components

The Query Orchestrator consists of several interconnected components:

1. **QueryOrchestrator** (`src/orchestrator/QueryOrchestrator.ts`): Main orchestration class that coordinates query execution and manages drivers
2. **QueryCache** (`src/orchestrator/QueryCache.ts`): Handles query result caching with configurable cache drivers
3. **QueryQueue** (`src/orchestrator/QueryQueue.ts`): Manages query queuing and background processing
4. **PreAggregations** (`src/orchestrator/PreAggregations.ts`): Manages pre-aggregation building and loading
5. **DriverFactory** (`src/orchestrator/DriverFactory.ts`): Creates and manages database driver instances

### Cache and Queue Driver Architecture

The orchestrator supports multiple backend drivers:
- **Memory**: In-memory caching and queuing (development)
- **CubeStore**: Distributed storage engine (production)
- **Redis**: External Redis-based caching (legacy, being phased out)

Driver selection logic in `QueryOrchestrator.ts:detectQueueAndCacheDriver()`:
- Explicit configuration via `cacheAndQueueDriver` option
- Environment variables (`CUBEJS_CACHE_AND_QUEUE_DRIVER`)
- Auto-detection: Redis if `CUBEJS_REDIS_URL` exists, CubeStore for production, Memory for development

### Query Processing Flow

1. **Query Submission**: Queries enter through QueryOrchestrator
2. **Cache Check**: QueryCache checks for existing results
3. **Queue Management**: QueryQueue handles background execution
4. **Pre-aggregation Processing**: PreAggregations component manages rollup tables
5. **Result Caching**: Results stored via cache driver for future requests

### Pre-aggregation System

The pre-aggregation system includes:
- **PreAggregationLoader**: Loads pre-aggregation definitions
- **PreAggregationPartitionRangeLoader**: Handles partition range loading
- **PreAggregationLoadCache**: Manages loading cache for pre-aggregations

## Testing Structure

### Unit Tests (`test/unit/`)
- `QueryCache.test.ts`: Query caching functionality
- `QueryQueue.test.ts`: Queue management and processing
- `QueryOrchestrator.test.js`: Main orchestrator logic
- `PreAggregations.test.js`: Pre-aggregation management

### Integration Tests (`test/integration/`)
- `cubestore/`: CubeStore-specific integration tests
- Tests real database interactions and queue processing

### Test Abstractions
- `QueryCache.abstract.ts`: Shared test suite for cache implementations
- `QueryQueue.abstract.ts`: Shared test suite for queue implementations

## Key Design Patterns

### Queue Processing Architecture
The DEVELOPMENT.md file contains detailed sequence diagrams showing:
- Queue interaction with CubeStore via specific queue commands (`QUEUE ADD`, `QUEUE RETRIEVE`, etc.)
- Background query processing with heartbeat management
- Result handling and cleanup

### Driver Factory Pattern
- `DriverFactory` type enables pluggable database drivers
- `DriverFactoryByDataSource` supports multi-tenant scenarios
- Separation between external (user data) and internal (cache/queue) drivers

### Error Handling
- `ContinueWaitError`: Signals when queries should continue waiting
- `TimeoutError`: Handles query timeout scenarios
- Proper cleanup and resource management across all components

## Configuration

Key configuration options in `QueryOrchestratorOptions`:
- `externalDriverFactory`: Database driver for user data
- `cacheAndQueueDriver`: Backend for caching and queuing
- `queryCacheOptions`: Cache-specific settings
- `preAggregationsOptions`: Pre-aggregation configuration
- `rollupOnlyMode`: When enabled, only serves pre-aggregated data
- `continueWaitTimeout`: Timeout for waiting operations

## Development Notes

- Uses TypeScript with relaxed strict settings (`tsconfig.json`)
- Inherits linting rules from `@cubejs-backend/linter`
- Jest configuration extends base repository config
- Docker Compose setup for integration testing
- Coverage reports generated in `coverage/` directory
