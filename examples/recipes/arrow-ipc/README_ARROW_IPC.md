# Arrow IPC Integration - Complete Documentation

This directory contains documentation and prototypes for integrating Arrow IPC (Inter-Process Communication) format with Cube, enabling high-performance binary data transfer.

## Overview

This project demonstrates how to stream data from CubeStore directly to cubesqld using Arrow IPC format, bypassing the Node.js Cube API HTTP/JSON layer for data transfer.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│ Client (BI Tools, Applications)                         │
└────────────────┬────────────────────────────────────────┘
                 │ PostgreSQL wire protocol
                 ↓
┌─────────────────────────────────────────────────────────┐
│ cubesqld (Rust)                                         │
│  - SQL parsing & query planning                         │
│  - cubesqlplanner (pre-aggregation selection)           │
│  - CubeStoreClient (direct WebSocket connection)        │
└─────────────┬──────────────────┬────────────────────────┘
              │                  │
     Metadata │          Data    │
     (HTTP)   │    (WebSocket +  │
              │     FlatBuffers  │
              │     → Arrow)     │
              ↓                  ↓
  ┌──────────────────┐  ┌──────────────────┐
  │  Cube API        │  │  CubeStore       │
  │  (Node.js)       │  │  (Rust)          │
  │                  │  │                  │
  │  - Metadata      │  │  - Pre-aggs      │
  │  - Security      │  │  - Query exec    │
  │  - Orchestration │  │  - Data storage  │
  └──────────────────┘  └──────────────────┘
```

## Key Documents

### 1. [CUBESTORE_DIRECT_PROTOTYPE.md](./CUBESTORE_DIRECT_PROTOTYPE.md)

**What it is**: Working prototype of cubesqld connecting directly to CubeStore

**Status**: ✅ Complete and working

**Key features**:
- WebSocket connection to CubeStore
- FlatBuffers protocol implementation
- FlatBuffers → Arrow RecordBatch conversion
- Type inference from string data
- NULL value handling
- Error handling and timeouts

**How to run**:
```bash
# Start CubeStore
./start-cubestore.sh

# Run the prototype
cd /home/io/projects/learn_erl/cube/rust/cubesql
cargo run --example cubestore_direct
```

**Files created**:
- `rust/cubesql/cubesql/src/cubestore/client.rs` (~310 lines)
- `rust/cubesql/cubesql/examples/cubestore_direct.rs` (~200 lines)

### 2. [HYBRID_APPROACH_PLAN.md](./HYBRID_APPROACH_PLAN.md)

**What it is**: Complete implementation plan for production integration

**Status**: 📋 Ready for implementation

**Key discovery**: Cube already has pre-aggregation selection logic in Rust!

**Timeline**: 2-3 weeks

**Key components**:
1. **CubeStoreTransport** - Direct data path via WebSocket
2. **Metadata caching** - Cache Cube API `/v1/meta` responses
3. **Security context** - Row-level security enforcement
4. **Pre-agg resolution** - Map semantic names → physical tables
5. **Fallback mechanism** - Automatic fallback to Cube API on errors

**Phases**:
- **Week 1**: Foundation (CubeStoreTransport, configuration)
- **Week 2**: Integration (metadata caching, security, testing)
- **Week 3**: Optimization (performance tuning, benchmarks)

### 3. [IMPLEMENTATION_PLAN.md](./IMPLEMENTATION_PLAN.md)

**What it is**: Earlier exploration of Option B (Hybrid with Schema Sync)

**Status**: ⚠️ Superseded by HYBRID_APPROACH_PLAN.md

**Note**: This was written before discovering the existing Rust pre-aggregation logic. The HYBRID_APPROACH_PLAN.md is the current, accurate plan.

## Key Findings

### Discovery: Existing Rust Pre-Aggregation Logic

During investigation, we discovered that **Cube already has a complete Rust implementation** of the pre-aggregation selection algorithm:

**Location**: `rust/cubesqlplanner/cubesqlplanner/src/logical_plan/optimizers/pre_aggregation/`

**Components** (~1,650 lines of Rust):
- `optimizer.rs` - Main pre-aggregation optimizer
- `pre_aggregations_compiler.rs` - Compiles pre-aggregation definitions
- `measure_matcher.rs` - Matches measures to pre-aggregations
- `dimension_matcher.rs` - Matches dimensions to pre-aggregations
- `compiled_pre_aggregation.rs` - Data structures

**Integration**:
```javascript
// packages/cubejs-schema-compiler/src/adapter/PreAggregations.ts:844-857
public findPreAggregationForQuery(): PreAggregationForQuery | undefined {
  if (this.query.useNativeSqlPlanner &&
      this.query.canUseNativeSqlPlannerPreAggregation) {
    // Uses Rust implementation via N-API! ✅
    this.preAggregationForQuery = this.query.findPreAggregationForQueryRust();
  } else {
    // Fallback to TypeScript
    this.preAggregationForQuery = this.rollupMatchResults().find(...);
  }
  return this.preAggregationForQuery;
}
```

**Implication**: We don't need to port ~4,000 lines of TypeScript - we can reuse the existing Rust implementation!

## Performance Benefits

### Current Flow (HTTP/JSON)
```
CubeStore → FlatBuffers → Node.js → JSON → HTTP → cubesqld → JSON parse → Arrow
           ↑____________ Row oriented ____________↑   ↑____ Columnar ____↑

Overhead: WebSocket→HTTP conversion, JSON serialization, string parsing
```

### Direct Flow (This Project)
```
CubeStore → FlatBuffers → cubesqld → Arrow
           ↑___ Row ___↑   ↑__ Columnar __↑

Benefits: Binary protocol, direct conversion, type inference, pre-allocated builders
```

**Expected improvements**:
- **Latency**: 30-50% reduction
- **Throughput**: 2-3x increase
- **Memory**: 40% less usage
- **CPU**: Less JSON parsing overhead

## Repository Structure

```
examples/recipes/arrow-ipc/
├── README_ARROW_IPC.md                    # This file - overview
├── CUBESTORE_DIRECT_PROTOTYPE.md          # Prototype documentation
├── HYBRID_APPROACH_PLAN.md                # Production implementation plan
├── IMPLEMENTATION_PLAN.md                 # Earlier exploration (superseded)
├── start-cubestore.sh                     # Helper script to start CubeStore
└── start-cube-api.sh                      # Helper script to start Cube API

rust/cubesql/cubesql/
├── src/
│   ├── cubestore/
│   │   ├── mod.rs                         # Module exports
│   │   └── client.rs                      # CubeStoreClient implementation
│   └── transport/                         # (To be created)
│       ├── cubestore.rs                   # CubeStoreTransport
│       ├── metadata_cache.rs              # Metadata caching
│       └── security_context.rs            # Security enforcement
└── examples/
    └── cubestore_direct.rs                # Standalone test example

rust/cubesqlplanner/cubesqlplanner/src/
└── logical_plan/optimizers/pre_aggregation/
    ├── optimizer.rs                       # Pre-agg selection logic
    ├── pre_aggregations_compiler.rs       # Pre-agg compilation
    ├── measure_matcher.rs                 # Measure matching
    ├── dimension_matcher.rs               # Dimension matching
    └── compiled_pre_aggregation.rs        # Data structures

packages/cubejs-backend-native/src/
├── node_export.rs                         # N-API exports to Node.js
└── ...                                    # Other bridge code
```

## Getting Started

### Prerequisites

1. **CubeStore running** at `localhost:3030`
2. **Cube API running** at `localhost:4000` (for metadata)
3. **Rust toolchain** installed (1.90.0+)

### Quick Start

1. **Start CubeStore**:
   ```bash
   cd examples/recipes/arrow-ipc
   ./start-cubestore.sh
   ```

2. **Run the prototype**:
   ```bash
   cd rust/cubesql
   cargo run --example cubestore_direct
   ```

3. **Expected output**:
   ```
   ==========================================
   CubeStore Direct Connection Test
   ==========================================
   Connecting to CubeStore at: ws://127.0.0.1:3030/ws

   Test 1: Querying information schema
   ------------------------------------------
   SQL: SELECT * FROM information_schema.tables LIMIT 5

   ✓ Query successful!
     Results: 1 batches
     Batch 0: 5 rows × 3 columns
     Schema:
       - table_schema (Utf8)
       - table_name (Utf8)
       - build_range_end (Utf8)
   ...
   ```

### Next Steps

1. **Review**: Read [HYBRID_APPROACH_PLAN.md](./HYBRID_APPROACH_PLAN.md)
2. **Implement**: Follow the 3-week implementation plan
3. **Test**: Run integration tests and benchmarks
4. **Deploy**: Roll out with feature flag

## Configuration

### Environment Variables

```bash
# Enable direct CubeStore connection
export CUBESQL_CUBESTORE_DIRECT=true

# CubeStore WebSocket URL
export CUBESQL_CUBESTORE_URL=ws://localhost:3030/ws

# Cube API URL (for metadata)
export CUBESQL_CUBE_URL=http://localhost:4000/cubejs-api
export CUBESQL_CUBE_TOKEN=your-token

# Metadata cache TTL (seconds)
export CUBESQL_METADATA_CACHE_TTL=300

# Logging
export CUBESQL_LOG_LEVEL=debug
```

## Testing

### Unit Tests
```bash
cd rust/cubesql
cargo test cubestore
```

### Integration Tests
```bash
cd rust/cubesql
cargo test --test cubestore_direct
```

### Benchmarks
```bash
cd rust/cubesql
cargo bench cubestore_direct
```

## Troubleshooting

### Connection Refused
```
✗ Query failed: WebSocket connection failed: ...
```

**Solution**: Ensure CubeStore is running:
```bash
netstat -an | grep 3030
./start-cubestore.sh
```

### Query Timeout
```
✗ Query failed: Query timeout
```

**Solution**: Increase timeout in `client.rs` or check CubeStore logs

### Type Inference Issues
```
Data shows wrong types (all strings when should be numbers)
```

**Solution**: Expected behavior - CubeStore returns strings. Proper schema will come from Cube API metadata in production.

## Contributing

### Code Style
- Follow Rust standard style (`cargo fmt`)
- Run clippy before committing (`cargo clippy`)
- Add tests for new features
- Update documentation

### Testing Requirements
- All new code must have unit tests
- Integration tests for new features
- Performance benchmarks for optimizations
- Security tests for authentication/authorization

## References

### External Documentation
- [Apache Arrow IPC Format](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc)
- [FlatBuffers Documentation](https://google.github.io/flatbuffers/)
- [WebSocket Protocol](https://datatracker.ietf.org/doc/html/rfc6455)

### Cube Documentation
- [Pre-Aggregations](https://cube.dev/docs/caching/pre-aggregations/getting-started)
- [CubeStore](https://cube.dev/docs/caching/using-pre-aggregations#pre-aggregations-storage)
- [Cube SQL API](https://cube.dev/docs/backend/sql)

### Related Code
- `packages/cubejs-cubestore-driver/` - Node.js CubeStore driver (reference implementation)
- `rust/cubestore/` - CubeStore source code
- `rust/cubesql/` - Cube SQL API source code
- `rust/cubesqlplanner/` - SQL planner and pre-aggregation optimizer

## Timeline

### ✅ Completed
- [x] Prototype CubeStore direct connection
- [x] FlatBuffers → Arrow conversion
- [x] WebSocket client implementation
- [x] Type inference from string data
- [x] Documentation of prototype
- [x] Discovery of existing Rust pre-agg logic
- [x] Hybrid Approach planning

### 🚧 In Progress
- [ ] None currently

### 📋 Planned (3-week timeline)
- [ ] Week 1: CubeStoreTransport implementation
- [ ] Week 1: Configuration and environment setup
- [ ] Week 1: Basic integration tests
- [ ] Week 2: Metadata caching layer
- [ ] Week 2: Security context integration
- [ ] Week 2: Pre-aggregation table name resolution
- [ ] Week 2: Comprehensive integration tests
- [ ] Week 3: Performance optimization
- [ ] Week 3: Benchmarking
- [ ] Week 3: Error handling and fallback
- [ ] Week 3: Production readiness review

## License

Apache 2.0 (same as Cube)

---

## Contact

For questions or issues related to this project:
- GitHub Issues: https://github.com/cube-js/cube/issues
- Cube Community Slack: https://cube.dev/community
- Documentation: https://cube.dev/docs

---

**Last Updated**: 2025-12-25

**Status**: Prototype complete ✅ | Production plan ready 📋 | Implementation pending 🚧
