# Arrow IPC Documentation Index

Complete documentation for the Arrow IPC implementation in CubeSQL.

---

## Quick Start

**Status**: ✅ **PRODUCTION READY**
**Performance**: Up to **18x faster** than HTTP API for complex queries

### Running CubeSQL with Arrow IPC

```bash
cd /home/io/projects/learn_erl/cube/rust/cubesql

CUBESQL_CUBESTORE_DIRECT=true \
CUBESQL_CUBE_URL=http://localhost:4008/cubejs-api \
CUBESQL_CUBESTORE_URL=ws://127.0.0.1:3030/ws \
CUBESQL_CUBE_TOKEN=test \
CUBESQL_PG_PORT=4444 \
CUBEJS_ARROW_PORT=4445 \
RUST_LOG=cubesql=info \
cargo run
```

### Running Tests

```bash
cd /home/io/projects/learn_erl/power-of-three
mix test test/power_of_three/focused_http_vs_arrow_test.exs
```

---

## Documentation

### 📘 [IMPLEMENTATION_SUMMARY.md](./IMPLEMENTATION_SUMMARY.md)
**Read this first!** High-level overview of the project:
- What was accomplished
- Files modified
- Technical fixes applied
- Performance benchmarks
- Testing instructions
- **Best for**: Project managers, new developers

### 📗 [ARROW_IPC_IMPLEMENTATION.md](./ARROW_IPC_IMPLEMENTATION.md)
Comprehensive technical guide:
- Architecture overview
- Pre-aggregation SQL generation
- Table discovery and selection
- Arrow IPC protocol details
- Troubleshooting guide
- **Best for**: Developers implementing features, debugging issues

### 📕 [SQL_GENERATION_INVESTIGATION.md](./SQL_GENERATION_INVESTIGATION.md)
Detailed investigation log:
- All issues discovered
- Hypotheses tested
- Fixes applied step-by-step
- The breakthrough moment
- Final resolution
- **Best for**: Understanding the debugging process, learning from mistakes

---

## Performance Summary

| Test | Arrow IPC | HTTP API | Speedup |
|------|-----------|----------|---------|
| Daily aggregation (50 rows) | 95ms | 43ms | HTTP faster |
| Monthly aggregation (100 rows) | **115ms** | 2,081ms | **18.1x faster** |
| Simple aggregation (20 rows) | **91ms** | 226ms | **2.48x faster** |

---

## Key Components

### Source Files

**Pre-Aggregation SQL Generation**:
- `/compile/engine/df/scan.rs` - `generate_pre_agg_sql()` function

**CubeStore Integration**:
- `/transport/cubestore_transport.rs` - Table discovery and SQL rewriting
- `/transport/hybrid_transport.rs` - Routing logic

**Arrow IPC Protocol**:
- `/sql/arrow_native/server.rs` - Protocol server
- `/sql/arrow_native/stream_writer.rs` - Batch streaming
- `/sql/arrow_native/protocol.rs` - Message encoding/decoding
- `/sql/arrow_ipc.rs` - Arrow IPC serialization

### Test Files

**Integration Tests**:
- `/power-of-three/test/power_of_three/focused_http_vs_arrow_test.exs`
- `/power-of-three/test/power_of_three/http_vs_arrow_comprehensive_test.exs`

---

## Common Tasks

### Debugging SQL Generation

Enable verbose logging:
```bash
RUST_LOG=cubesql=debug,cubesql::transport=trace cargo run
```

Look for these log messages:
- `🚀 Generated SQL for pre-agg` - See generated SQL
- `Selected pre-agg table:` - Which table was chosen
- `📦 Arrow Flight batch #N` - Batch streaming progress

### Inspecting Pre-Aggregation Tables

Query CubeStore directly:
```bash
PGPASSWORD=test psql -h 127.0.0.1 -p 4444 -U root -d db \
  -c "SELECT table_schema, table_name, created_at
      FROM system.tables
      WHERE is_ready = true
      ORDER BY created_at DESC
      LIMIT 10"
```

### Testing Specific SQL

Via PostgreSQL protocol:
```bash
PGPASSWORD=test psql -h 127.0.0.1 -p 4444 -U root -d db \
  -c "SELECT market_code, MEASURE(count)
      FROM orders_with_preagg
      GROUP BY 1
      ORDER BY 2 DESC
      LIMIT 10"
```

---

## Troubleshooting

### "No field named X"
**Cause**: Missing granularity suffix
**Fix**: Add pre-agg granularity to field name (e.g., `updated_at_day`)

### Wrong Row Counts
**Cause**: Using old table version
**Fix**: Verify `ORDER BY created_at DESC` in table discovery

### Test Counting Errors
**Cause**: Counting columns instead of rows
**Fix**: Use `length(Adbc.Column.to_list(first_col))`, not `length(result.data)`

---

## Related Work

### CubeStore
Pre-aggregation tables are managed by CubeStore:
- Location: `/rust/cubestore/`
- System tables: `/cubestore/src/queryplanner/info_schema/system_tables.rs`

### Cube.js HTTP API
The traditional query path:
- Client → HTTP API → CubeStore
- Uses REST API with JSON responses
- Good for simple queries, slower for complex aggregations

### Arrow IPC Direct Path
The new optimized path:
- Client → CubeSQL (Arrow IPC) → CubeStore
- Uses Arrow columnar format
- Ideal for analytical queries with complex aggregations

---

## Contributing

When modifying the Arrow IPC implementation:

1. **Update SQL generation** in `/compile/engine/df/scan.rs`
   - Document any changes to field naming
   - Add tests for new query patterns

2. **Update protocol** in `/sql/arrow_native/`
   - Maintain backwards compatibility
   - Update protocol version if breaking changes

3. **Update documentation**
   - Add examples to `ARROW_IPC_IMPLEMENTATION.md`
   - Document troubleshooting steps

4. **Run tests**
   ```bash
   cargo test arrow_ipc
   mix test test/power_of_three/focused_http_vs_arrow_test.exs
   ```

---

## Questions?

For detailed technical information, see:
- **Architecture**: `ARROW_IPC_IMPLEMENTATION.md`
- **Investigation**: `SQL_GENERATION_INVESTIGATION.md`
- **Summary**: `IMPLEMENTATION_SUMMARY.md`

---

**Last Updated**: 2025-12-26
**Status**: ✅ Production Ready
**Performance**: Up to 18x faster than HTTP API
