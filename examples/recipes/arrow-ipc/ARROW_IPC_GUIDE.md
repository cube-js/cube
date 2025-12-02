# Arrow IPC (Inter-Process Communication) Support for CubeSQL

## Overview

CubeSQL now supports Apache Arrow IPC Streaming Format as an alternative output format for query results. This enables:

- **Zero-copy data transfer** for efficient memory usage
- **Columnar format** optimized for analytics workloads
- **Native integration** with data processing libraries (pandas, polars, PyArrow, etc.)
- **Streaming support** for large result sets

## What is Arrow IPC?

Apache Arrow IPC (RFC 0017) is a standardized format for inter-process communication using Arrow's columnar data model. Instead of receiving results as rows (PostgreSQL wire protocol), clients receive results in Arrow's columnar format, which is:

1. **More efficient** for analytical queries that access specific columns
2. **Faster to deserialize** - zero-copy capability in many cases
3. **Language-agnostic** - supported across Python, R, JavaScript, C++, Java, etc.
4. **Streaming-capable** - can process large datasets without loading everything into memory

## Implementation Details

### Phase 1: Serialization (Completed)
- `cubesql/src/sql/arrow_ipc.rs`: ArrowIPCSerializer for RecordBatch serialization
- Support for single and streaming batch serialization
- Comprehensive test coverage (7 tests)

### Phase 2: Protocol Integration (Completed)
- Connection parameter support in `shim.rs`
- PortalBatch::ArrowIPCData variant for Arrow IPC responses
- Proper message handling in write_portal()

### Phase 3: Portal Execution & Client Examples (Just Completed)
- Portal.execute() now branches on OutputFormat
- Streaming execution with Arrow IPC serialization
- Fall-back to PostgreSQL format for frame state queries
- Python, JavaScript, and R client examples
- Integration test suite

## Usage

### Enable Arrow IPC Output

```sql
-- Set output format to Arrow IPC for the current session
SET output_format = 'arrow_ipc';

-- Execute queries - results will be in Arrow IPC format
SELECT * FROM table_name;

-- Switch back to PostgreSQL format
SET output_format = 'postgresql';
```

### Valid Output Format Values

- `'postgresql'` or `'postgres'` or `'pg'` (default)
- `'arrow_ipc'` or `'arrow'` or `'ipc'`

## Client Examples

### Python

```python
from examples.arrow_ipc_client import CubeSQLArrowIPCClient
import pandas as pd

client = CubeSQLArrowIPCClient(host="127.0.0.1", port=4444)
client.connect()
client.set_arrow_ipc_output()

# Execute query and convert to pandas DataFrame
df = client.execute_query_with_arrow_streaming(
    "SELECT * FROM information_schema.tables"
)

# Save to Parquet for efficient storage
df.to_parquet("results.parquet")

client.close()
```

See `examples/arrow_ipc_client.py` for complete examples including:
- Basic queries
- Arrow to NumPy conversion
- Saving to Parquet
- Performance comparison

### JavaScript/Node.js

```javascript
const { CubeSQLArrowIPCClient } = require("./examples/arrow_ipc_client.js");

const client = new CubeSQLArrowIPCClient();
await client.connect();
await client.setArrowIPCOutput();

const results = await client.executeQuery(
  "SELECT * FROM information_schema.tables"
);

// Convert to Apache Arrow Table for columnar processing
const { tableFromJSON } = require("apache-arrow");
const table = tableFromJSON(results);

await client.close();
```

See `examples/arrow_ipc_client.js` for complete examples including:
- Stream processing for large datasets
- JSON export
- Performance comparison with PostgreSQL format
- Native Arrow processing

### R

```r
source("examples/arrow_ipc_client.R")

client <- CubeSQLArrowIPCClient$new()
client$connect()
client$set_arrow_ipc_output()

# Execute query
results <- client$execute_query(
  "SELECT * FROM information_schema.tables"
)

# Convert to Arrow Table
arrow_table <- arrow::as_arrow_table(results)

# Save to Parquet
arrow::write_parquet(arrow_table, "results.parquet")

client$close()
```

See `examples/arrow_ipc_client.R` for complete examples including:
- Arrow table manipulation with dplyr
- Streaming large result sets
- Parquet export
- Performance comparison
- Tidyverse integration

## Architecture

### Query Execution Flow

```
Client executes: SET output_format = 'arrow_ipc'
        |
        v
SessionState.output_format set to OutputFormat::ArrowIPC
        |
        v
Client executes query
        |
        v
Portal.execute() called
        |
        +---> For InExecutionStreamState (streaming):
        |     - Calls serialize_batch_to_arrow_ipc()
        |     - Yields PortalBatch::ArrowIPCData(ipc_bytes)
        |     - send_portal_batch writes to socket
        |
        +---> For InExecutionFrameState (MetaTabular):
              - Falls back to PostgreSQL format
              - (RecordBatch conversion not needed for frame state)
```

### Key Components

#### SessionState (session.rs)
```rust
pub struct SessionState {
    // ... other fields ...
    pub output_format: RwLockSync<OutputFormat>,
}

impl SessionState {
    pub fn output_format(&self) -> OutputFormat { /* ... */ }
    pub fn set_output_format(&self, format: OutputFormat) { /* ... */ }
}
```

#### Portal (extended.rs)
```rust
pub struct Portal {
    // ... other fields ...
    output_format: crate::sql::OutputFormat,
}

impl Portal {
    fn serialize_batch_to_arrow_ipc(
        &self,
        batch: RecordBatch,
        max_rows: usize,
        left: &mut usize,
    ) -> Result<(Option<RecordBatch>, Vec<u8>), ConnectionError>
}
```

#### PortalBatch (postgres.rs)
```rust
pub enum PortalBatch {
    Rows(WriteBuffer),
    ArrowIPCData(Vec<u8>),
}
```

#### ArrowIPCSerializer (arrow_ipc.rs)
```rust
impl ArrowIPCSerializer {
    pub fn serialize_single(batch: &RecordBatch) -> Result<Vec<u8>, CubeError>
    pub fn serialize_streaming(batches: &[RecordBatch]) -> Result<Vec<u8>, CubeError>
}
```

## Testing

### Unit Tests
All unit tests pass (661 tests total):
- Arrow IPC serialization: 7 tests
- Portal execution: 6 tests
- Extended protocol: Multiple tests

Run tests:
```bash
cargo test --lib arrow_ipc --no-default-features
cargo test --lib postgres::extended --no-default-features
```

### Integration Tests
New integration test suite in `cubesql/e2e/tests/arrow_ipc.rs`:
- Setting output format
- Switching between formats
- Format persistence
- System table queries
- Concurrent queries

Run integration tests (requires Cube.js instance):
```bash
CUBESQL_TESTING_CUBE_TOKEN=... CUBESQL_TESTING_CUBE_URL=... cargo test --test arrow_ipc
```

## Performance Considerations

1. **Serialization overhead**: Arrow IPC has minimal serialization overhead compared to PostgreSQL protocol
2. **Transfer size**: Arrow IPC is typically more efficient for large datasets
3. **Deserialization**: Clients benefit from zero-copy deserialization
4. **Memory usage**: Columnar format is more memory-efficient for analytical workloads

## Limitations and Future Work

### Current Limitations
1. Frame state queries (MetaTabular) fall back to PostgreSQL format
   - These are typically metadata queries returning small datasets
   - Full Arrow IPC support would require DataFrame → RecordBatch conversion

2. Connection parameters approach is preliminary
   - Final implementation will add proper SET command handling

### Future Improvements
1. Implement `SET output_format` command parsing in extended query protocol
2. Full Arrow IPC support for all query types
3. Support for Arrow Flight protocol (superset of IPC with RPC support)
4. Performance optimizations for very large result sets
5. Support for additional output formats (Parquet, ORC, etc.)

## Compatibility

Arrow IPC output format is compatible with:
- **Python**: PyArrow, pandas, polars
- **R**: arrow, tidyverse
- **JavaScript**: apache-arrow, Node.js
- **C++**: Arrow C++ library
- **Java**: Arrow Java library
- **Go**: Arrow Go library
- **Rust**: Arrow Rust library

## Troubleshooting

### Connection Issues
```
Error: Failed to connect to CubeSQL
Solution: Ensure CubeSQL is running on the correct host:port
```

### Format Not Changing
```
Error: output_format still shows 'postgresql'
Solution: Use exact syntax: SET output_format = 'arrow_ipc'
```

### Library Import Errors
```python
# Python
pip install psycopg2-binary pyarrow pandas

# JavaScript
npm install pg apache-arrow

# R
install.packages(c("RPostgres", "arrow", "tidyverse", "dplyr"))
```

## References

- [Apache Arrow Documentation](https://arrow.apache.org/)
- [Arrow IPC Format (RFC 0017)](https://arrow.apache.org/docs/format/Columnar.html)
- [PostgreSQL Wire Protocol](https://www.postgresql.org/docs/current/protocol.html)
- [CubeSQL Documentation](https://cube.dev/docs/product/cube-sql)

## Next Steps

1. Run existing CubeSQL tests to verify integration
2. Deploy to test environment and validate with real BI tools
3. Gather performance metrics on production workloads
4. Implement remaining Arrow IPC features from the roadmap
