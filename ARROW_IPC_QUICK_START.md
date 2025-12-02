# Arrow IPC Implementation - Quick Start Guide

Fast-track guide to implementing Arrow IPC data access in Cube.

## TL;DR

**What**: Add Arrow IPC as an output format option alongside PostgreSQL protocol in CubeSQL

**Why**: Enable zero-copy data access via Arrow ecosystem (PyArrow, Arrow R, Arrow JS, DuckDB, Pandas, etc.)

**How long**: 5 weeks in 4 phases, ~90 hours total

**Difficulty**: Medium (reuses existing Arrow IPC code from CubeStore)

**Value**: Unlocks streaming analytics, zero-copy processing, Arrow ecosystem integration

---

## Current State

```
CubeSQL
  ├─ Input: SQL queries (PostgreSQL protocol)
  └─ Output: PostgreSQL wire protocol ONLY
             (internally uses Arrow RecordBatch)
```

## Desired State

```
CubeSQL
  ├─ Input: SQL queries
  │  ├─ PostgreSQL protocol
  │  └─ + Arrow IPC protocol (NEW)
  │
  └─ Output:
     ├─ PostgreSQL protocol (default)
     ├─ Arrow IPC (NEW)
     └─ JSON (optional)
```

---

## Why Arrow IPC?

### Current Flow: PostgreSQL Protocol

```
RecordBatch (Arrow columnar)
  → Extract arrays
  → Convert each value to PostgreSQL format
  → Send binary data
  → Client parses PostgreSQL format
  → Convert back to app-specific format
  ❌ Multiple conversions, serialization overhead
```

### Proposed Flow: Arrow IPC

```
RecordBatch (Arrow columnar)
  → Serialize to Arrow IPC format
  → Send binary data
  → Client parses Arrow IPC
  → Use directly in PyArrow/Pandas/DuckDB/etc.
  ✅ Single conversion, native format, zero-copy capable
```

### Benefits

| Feature | PostgreSQL | Arrow IPC |
|---------|-----------|-----------|
| Efficiency | Medium | **High** |
| Zero-copy | ❌ | ✅ |
| Streaming | ❌ | ✅ |
| Large datasets | ❌ | ✅ |
| Arrow ecosystem | ❌ | ✅ |
| Standard format | ❌ | ✅ (RFC 0017) |

---

## Implementation Phases

### Phase 1: Serialization (1 week, 20 hours)

**Goal**: Basic Arrow IPC output capability

**Files to Create/Modify**:
```
rust/cubesql/cubesql/src/sql/arrow_ipc.rs          (NEW)
rust/cubesql/cubesql/src/server/session.rs         (MODIFY)
rust/cubesql/cubesql/src/sql/response.rs           (MODIFY)
```

**Code Changes** (~100 lines total):

```rust
// 1. Add to session.rs (5 lines)
pub enum OutputFormat {
    PostgreSQL,
    ArrowIPC,
    JSON,
}

// 2. Create arrow_ipc.rs (40 lines)
pub struct ArrowIPCSerializer;

impl ArrowIPCSerializer {
    pub fn serialize_streaming(batches: &[RecordBatch])
        -> Result<Vec<u8>> {
        let schema = batches[0].schema();
        let mut output = Vec::new();
        let mut writer = MemStreamWriter::try_new(
            Cursor::new(&mut output),
            schema,
        )?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.finish()?;
        Ok(output)
    }
}

// 3. Modify response.rs (15 lines)
match session.output_format {
    OutputFormat::PostgreSQL =>
        encode_postgres_protocol(&batches, socket).await,
    OutputFormat::ArrowIPC => {
        let ipc = ArrowIPCSerializer::serialize_streaming(&batches)?;
        socket.write_all(&ipc).await?;
        Ok(())
    },
}
```

**Test**:
```rust
#[test]
fn test_arrow_ipc_roundtrip() {
    let batches = vec![/* test data */];
    let ipc = ArrowIPCSerializer::serialize_streaming(&batches).unwrap();

    let reader = StreamReader::try_new(Cursor::new(ipc)).unwrap();
    let result: Vec<_> = reader.collect::<Result<_,_>>().unwrap();

    assert_eq!(result[0].num_rows(), batches[0].num_rows());
}
```

**Deliverable**: Working serialization, testable via unit tests

---

### Phase 2: Connection Parameters (1 week, 15 hours)

**Goal**: Allow clients to request Arrow IPC format

**Options** (pick one or combine):

**Option A: Connection String Parameter**
```
postgresql://localhost:5432/?output_format=arrow_ipc
```

**Option B: SET Command**
```sql
SET output_format = 'arrow_ipc';
SELECT * FROM orders;
```

**Option C: HTTP Header (for HTTP clients)**
```http
GET /api/v1/load?output_format=arrow_ipc
Content-Type: application/x-arrow-ipc
```

**Files to Modify**:
```
rust/cubesql/cubesql/src/server/connection.rs  (MODIFY)
rust/cubesql/cubesql/src/sql/ast.rs            (MODIFY)
```

**Implementation**:

```rust
// In connection.rs
fn parse_connection_string(connstr: &str) -> Result<SessionConfig> {
    let params = parse_url_params(connstr);
    let output_format = params.get("output_format")
        .map(|f| match f.as_str() {
            "arrow_ipc" => OutputFormat::ArrowIPC,
            "json" => OutputFormat::JSON,
            _ => OutputFormat::PostgreSQL,
        })
        .unwrap_or(OutputFormat::PostgreSQL);

    Ok(SessionConfig { output_format, ... })
}
```

**Deliverable**: Clients can specify output format, tests pass

---

### Phase 3: Client Libraries (1 week, 25 hours)

**Goal**: Working examples in multiple languages

**Python Example** (5 minutes):
```python
import socket
import pyarrow as pa

# Connect to CubeSQL
sock = socket.socket()
sock.connect(("localhost", 5432))

# Send query with Arrow IPC format
query = b"""SELECT status, SUM(amount) FROM orders
           GROUP BY status FORMAT arrow_ipc"""
sock.send(query)

# Receive Arrow IPC data
data = sock.recv(1000000)

# Parse with PyArrow
reader = pa.RecordBatchStreamReader(pa.BufferReader(data))
table = reader.read_all()

# Use in Pandas
df = table.to_pandas()
print(df)
```

**JavaScript Example** (5 minutes):
```javascript
import * as arrow from 'apache-arrow';

const socket = new WebSocket('ws://localhost:5432');

socket.send(JSON.stringify({
    query: 'SELECT * FROM orders',
    format: 'arrow_ipc'
}));

socket.onmessage = (event) => {
    const buffer = event.data;
    const reader = new arrow.RecordBatchReader(buffer);
    const table = reader.readAll();

    console.log(table.toArray());
};
```

**R Example** (5 minutes):
```r
library(arrow)

# Connect and query
con <- DBI::dbConnect(
    RPostgres::Postgres(),
    host = "localhost",
    dbname = "cube",
    output_format = "arrow_ipc"
)

# Query returns Arrow Table directly
table <- DBI::dbGetQuery(con,
    "SELECT * FROM orders")

# Use in R
as.data.frame(table)
```

**Files to Create**:
```
examples/arrow-ipc-client-python.py
examples/arrow-ipc-client-js.js
examples/arrow-ipc-client-r.R
docs/arrow-ipc-guide.md
```

**Deliverable**: Working examples, documentation, can fetch data in Arrow format

---

### Phase 4: Advanced Features (2 weeks, 30 hours)

**Goal**: Production-ready with optimization and advanced features

**Features**:

1. **Streaming Support** (for large datasets)
   - Incremental Arrow IPC messages
   - Client can start processing while receiving
   - Support 1GB+ datasets

2. **Compression** (Arrow-compatible)
   - LZ4, Zstd compression for network
   - Transparent decompression on client

3. **Schema Evolution**
   - Handle schema changes between batches
   - Metadata versioning

4. **Performance Optimization**
   - Batch size tuning
   - Memory-mapped buffers
   - Zero-copy for suitable data types

**Implementation**:

```rust
// Streaming version
pub async fn stream_arrow_ipc(
    batches: impl Stream<Item=RecordBatch>,
    socket: &mut TcpStream,
) -> Result<()> {
    let mut schema_sent = false;

    for batch in batches {
        if !schema_sent {
            // Send schema once
            let schema = batch.schema();
            send_arrow_schema(schema, socket).await?;
            schema_sent = true;
        }

        // Send each batch incrementally
        let ipc = ArrowIPCSerializer::serialize_streaming(&[batch])?;
        socket.write_all(&ipc).await?;
    }

    Ok(())
}

// Compression wrapper
pub fn compress_arrow_ipc(
    data: &[u8],
    codec: CompressionCodec,
) -> Result<Vec<u8>> {
    match codec {
        CompressionCodec::LZ4 => lz4_compress(data),
        CompressionCodec::Zstd => zstd_compress(data),
        CompressionCodec::None => Ok(data.to_vec()),
    }
}
```

**Deliverable**: Production-ready implementation, all features working

---

## Code Locations (Reference Implementation)

### CubeStore Already Has Arrow IPC!

**File**: `/rust/cubestore/cubestore/src/queryplanner/query_executor.rs`

```rust
pub struct SerializedRecordBatchStream {
    #[serde(with = "serde_bytes")]
    record_batch_file: Vec<u8>,
}

impl SerializedRecordBatchStream {
    pub fn write(
        schema: &Schema,
        record_batches: Vec<RecordBatch>,
    ) -> Result<Vec<Self>, CubeError> {
        // ... Arrow IPC serialization code ...
    }

    pub fn read(self) -> Result<RecordBatch, CubeError> {
        // ... Arrow IPC deserialization code ...
    }
}
```

**Use this as reference!** (Already proven to work)

### CubeSQL Response Handling

**File**: `/rust/cubesql/cubesql/src/sql/postgres/writer.rs`

```rust
// Shows how to extract arrays from RecordBatch
// and convert to output format

pub async fn write_query_result(
    record_batch: &RecordBatch,
    socket: &mut TcpStream,
) -> Result<()> {
    // Extract arrays
    for col in record_batch.columns() {
        // Convert each array to PostgreSQL format
    }
}
```

**Build on top of this!**

---

## Testing Strategy

### Unit Tests (Phase 1)
```rust
#[test]
fn test_serialize_to_arrow_ipc() { ... }

#[test]
fn test_roundtrip_arrow_ipc() { ... }

#[test]
fn test_arrow_ipc_all_types() { ... }
```

### Integration Tests (Phase 2)
```rust
#[tokio::test]
async fn test_query_with_arrow_ipc_output() { ... }

#[tokio::test]
async fn test_connection_parameter_parsing() { ... }
```

### E2E Tests (Phase 3)
```python
def test_pyarrow_client():
    # Connect, query, verify with PyArrow
    pass

def test_streaming_large_dataset():
    # Test 1GB+ dataset
    pass
```

---

## Success Metrics

| Metric | Target | How to Measure |
|--------|--------|---|
| Serialization | <5ms for 100k rows | Benchmark |
| Compatibility | Works with PyArrow, Arrow R, Arrow JS | Tests |
| Backward compatibility | 100% | All existing tests pass |
| Documentation | Complete | Docs review |
| Examples | 3+ languages | Client examples work |

---

## Estimated Effort

| Phase | Task | Hours | FTE Weeks |
|-------|------|-------|-----------|
| 1 | Core serialization | 20 | 0.5 |
| 2 | Parameters | 15 | 0.4 |
| 3 | Clients | 25 | 0.6 |
| 4 | Optimization | 30 | 0.75 |
| **Total** | | **90** | **2.25** |

**Real calendar time**: 5 weeks (with testing, reviews, iteration)

---

## Quick Implementation Checklist

### Phase 1 ✅
- [ ] Create `arrow_ipc.rs` with `ArrowIPCSerializer`
- [ ] Add `OutputFormat` enum to session
- [ ] Modify response handler
- [ ] Write unit tests
- [ ] Verify serialization roundtrip

### Phase 2 ✅
- [ ] Parse connection string parameters
- [ ] Handle `SET output_format` command
- [ ] Add integration tests
- [ ] Document configuration options

### Phase 3 ✅
- [ ] Create Python client example
- [ ] Create JavaScript client example
- [ ] Create R client example
- [ ] Write guide documentation

### Phase 4 ✅
- [ ] Implement streaming support
- [ ] Add compression
- [ ] Performance optimization
- [ ] Create benchmark suite

---

## Next Steps

1. **Review** the full analysis: `ARROW_IPC_ARCHITECTURE_ANALYSIS.md`
2. **Examine** CubeStore's reference implementation
3. **Start** Phase 1 (serialization)
4. **Test** with PyArrow
5. **Iterate** to Phase 2, 3, 4

---

## Key Insights

✅ **Arrow IPC already exists** in CubeStore
✅ **RecordBatch** is universal format (no conversion needed)
✅ **~200 lines of new code** needed for basic implementation
✅ **No new dependencies** required
✅ **Fully backward compatible**
✅ **Big value** for analytics workflows

---

## Resources

- **Arrow IPC RFC**: https://arrow.apache.org/docs/format/IPC.html
- **DataFusion Docs**: https://datafusion.apache.org/
- **Arrow Specifications**: https://arrow.apache.org/docs/

---

**Ready to start? Implement Phase 1 first!**
