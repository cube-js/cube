# CubeSQL → CubeStore Direct Connection Prototype

## Implementation Plan: Minimal Proof-of-Concept

### Goal
Create a minimal working prototype (~200-300 lines) that demonstrates cubesqld can query CubeStore directly via WebSocket and return Arrow IPC to clients, bypassing Cube API for data transfer.

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────┐
│ Client (Python/R/JS with Arrow)                          │
└────────────────┬─────────────────────────────────────────┘
                 │ Arrow IPC stream
                 ↓
┌──────────────────────────────────────────────────────────┐
│ cubesqld (Rust)                                          │
│  ┌────────────────────────────────────────────────────┐  │
│  │ New: CubeStoreClient                               │  │
│  │  - WebSocket connection                            │  │
│  │  - FlatBuffers encoding/decoding                   │  │
│  │  - FlatBuffers → Arrow conversion                  │  │
│  └────────────────────────────────────────────────────┘  │
└────────────────┬─────────────────────────────────────────┘
                 │ WebSocket + FlatBuffers
                 ↓
┌──────────────────────────────────────────────────────────┐
│ CubeStore                                                │
│  - WebSocket server at ws://localhost:3030/ws           │
│  - Returns HttpResultSet (FlatBuffers)                   │
└──────────────────────────────────────────────────────────┘
```

---

## Phase 1: Dependencies & Setup

### 1.1 Check/Add Dependencies

**File**: `/rust/cubesql/cubesql/Cargo.toml`

**Dependencies to verify/add**:
```toml
[dependencies]
tokio-tungstenite = "0.20"
futures-util = "0.3"
flatbuffers = "23.1.21"  # Already present
uuid = { version = "1.0", features = ["v4"] }
arrow = "50.0"  # Already present
```

**Action**: Read Cargo.toml, add only if missing

---

## Phase 2: CubeStore WebSocket Client

### 2.1 Create New Module

**File**: `/rust/cubesql/cubesql/src/cubestore/mod.rs` (new file)

```rust
pub mod client;
```

**File**: `/rust/cubesql/cubesql/src/cubestore/client.rs` (new file)

**Structure** (~150 lines):
```rust
use tokio_tungstenite::{connect_async, tungstenite::Message};
use futures_util::{SinkExt, StreamExt};
use flatbuffers::FlatBufferBuilder;
use arrow::{
    array::*,
    datatypes::*,
    record_batch::RecordBatch,
};
use std::sync::{Arc, atomic::{AtomicU32, Ordering}};

// Import FlatBuffers generated code
use crate::CubeError;
use cubeshared::codegen::http_message::*;

pub struct CubeStoreClient {
    url: String,
    connection_id: String,
    message_counter: AtomicU32,
}

impl CubeStoreClient {
    pub fn new(url: String) -> Self { ... }

    pub async fn query(&self, sql: String) -> Result<Vec<RecordBatch>, CubeError> { ... }

    fn build_query_message(&self, sql: &str) -> Vec<u8> { ... }

    fn flatbuffers_to_arrow(
        &self,
        result_set: HttpResultSet
    ) -> Result<Vec<RecordBatch>, CubeError> { ... }
}
```

### 2.2 FlatBuffers Message Building

**Key implementation details**:

```rust
fn build_query_message(&self, sql: &str) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();

    // Build query string
    let query_str = builder.create_string(sql);
    let conn_id_str = builder.create_string(&self.connection_id);

    // Build HttpQuery
    let query_obj = HttpQuery::create(&mut builder, &HttpQueryArgs {
        query: Some(query_str),
        trace_obj: None,
        inline_tables: None,
    });

    // Build HttpMessage wrapper
    let msg_id = self.message_counter.fetch_add(1, Ordering::SeqCst);
    let message = HttpMessage::create(&mut builder, &HttpMessageArgs {
        message_id: msg_id,
        command_type: HttpCommand::HttpQuery,
        command: Some(query_obj.as_union_value()),
        connection_id: Some(conn_id_str),
    });

    builder.finish(message, None);
    builder.finished_data().to_vec()
}
```

### 2.3 FlatBuffers → Arrow Conversion

**Type mapping strategy**:

```rust
fn infer_arrow_type(&self, rows: &Vector<ForwardsUOffset<HttpRow>>, col_idx: usize) -> DataType {
    // Sample first non-null value to infer type
    // CubeStore returns all values as strings in FlatBuffers
    // We need to infer the actual type by parsing

    for row in rows {
        let values = row.values().unwrap();
        let value = values.get(col_idx);

        if let Some(s) = value.string_value() {
            // Try parsing as different types
            if s.parse::<i64>().is_ok() {
                return DataType::Int64;
            } else if s.parse::<f64>().is_ok() {
                return DataType::Float64;
            } else if s == "true" || s == "false" {
                return DataType::Boolean;
            }
            // Default to string
            return DataType::Utf8;
        }
    }

    DataType::Utf8 // Default
}

fn flatbuffers_to_arrow(
    &self,
    result_set: HttpResultSet
) -> Result<Vec<RecordBatch>, CubeError> {
    let columns = result_set.columns().unwrap();
    let rows = result_set.rows().unwrap();

    if rows.len() == 0 {
        // Empty result set
        let fields: Vec<Field> = columns.iter()
            .map(|col| Field::new(col, DataType::Utf8, true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let empty_batch = RecordBatch::new_empty(schema);
        return Ok(vec![empty_batch]);
    }

    // Infer schema from data
    let fields: Vec<Field> = columns.iter()
        .enumerate()
        .map(|(idx, col)| {
            let dtype = self.infer_arrow_type(&rows, idx);
            Field::new(col, dtype, true)
        })
        .collect();
    let schema = Arc::new(Schema::new(fields));

    // Build columnar arrays
    let arrays = self.build_columnar_arrays(&schema, &rows)?;

    let batch = RecordBatch::try_new(schema, arrays)?;
    Ok(vec![batch])
}

fn build_columnar_arrays(
    &self,
    schema: &SchemaRef,
    rows: &Vector<ForwardsUOffset<HttpRow>>
) -> Result<Vec<ArrayRef>, CubeError> {
    let mut arrays = Vec::new();

    for (col_idx, field) in schema.fields().iter().enumerate() {
        let array: ArrayRef = match field.data_type() {
            DataType::Utf8 => {
                let mut builder = StringBuilder::new();
                for row in rows {
                    let values = row.values().unwrap();
                    let value = values.get(col_idx);
                    match value.string_value() {
                        Some(s) => builder.append_value(s),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Int64 => {
                let mut builder = Int64Builder::new();
                for row in rows {
                    let values = row.values().unwrap();
                    let value = values.get(col_idx);
                    match value.string_value() {
                        Some(s) => {
                            match s.parse::<i64>() {
                                Ok(n) => builder.append_value(n),
                                Err(_) => builder.append_null(),
                            }
                        }
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Float64 => {
                let mut builder = Float64Builder::new();
                for row in rows {
                    let values = row.values().unwrap();
                    let value = values.get(col_idx);
                    match value.string_value() {
                        Some(s) => {
                            match s.parse::<f64>() {
                                Ok(n) => builder.append_value(n),
                                Err(_) => builder.append_null(),
                            }
                        }
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Boolean => {
                let mut builder = BooleanBuilder::new();
                for row in rows {
                    let values = row.values().unwrap();
                    let value = values.get(col_idx);
                    match value.string_value() {
                        Some(s) => {
                            match s.to_lowercase().as_str() {
                                "true" | "t" | "1" => builder.append_value(true),
                                "false" | "f" | "0" => builder.append_value(false),
                                _ => builder.append_null(),
                            }
                        }
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            _ => {
                // Fallback: treat as string
                let mut builder = StringBuilder::new();
                for row in rows {
                    let values = row.values().unwrap();
                    let value = values.get(col_idx);
                    match value.string_value() {
                        Some(s) => builder.append_value(s),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
        };

        arrays.push(array);
    }

    Ok(arrays)
}
```

---

## Phase 3: Module Registration

### 3.1 Register Module in Main

**File**: `/rust/cubesql/cubesql/src/lib.rs`

**Add**:
```rust
pub mod cubestore;
```

**Action**: Add this line to the module declarations section

---

## Phase 4: Simple Test Binary

### 4.1 Create Standalone Test

**File**: `/rust/cubesql/cubesql/examples/cubestore_direct.rs` (new file)

```rust
use cubesql::cubestore::client::CubeStoreClient;
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let cubestore_url = env::var("CUBESQL_CUBESTORE_URL")
        .unwrap_or_else(|_| "ws://127.0.0.1:3030/ws".to_string());

    println!("Connecting to CubeStore at {}", cubestore_url);

    let client = CubeStoreClient::new(cubestore_url);

    // Simple test query
    let sql = "SELECT * FROM information_schema.tables LIMIT 5";
    println!("Executing: {}", sql);

    let batches = client.query(sql.to_string()).await?;

    println!("\nResults:");
    println!("  {} batches", batches.len());
    for (i, batch) in batches.iter().enumerate() {
        println!("  Batch {}: {} rows × {} columns",
            i, batch.num_rows(), batch.num_columns());

        // Print schema
        println!("  Schema:");
        for field in batch.schema().fields() {
            println!("    - {} ({})", field.name(), field.data_type());
        }

        // Print first few rows
        println!("  Data (first 3 rows):");
        let num_rows = batch.num_rows().min(3);
        for row_idx in 0..num_rows {
            print!("    [");
            for col_idx in 0..batch.num_columns() {
                let column = batch.column(col_idx);
                let value = format!("{:?}", column.slice(row_idx, 1));
                print!("{}", value);
                if col_idx < batch.num_columns() - 1 {
                    print!(", ");
                }
            }
            println!("]");
        }
    }

    Ok(())
}
```

**Run with**:
```bash
cargo run --example cubestore_direct
```

---

## Phase 5: Integration with Existing cubesqld

### 5.1 Add Transport Implementation (Optional for Prototype)

**File**: `/rust/cubesql/cubesql/src/transport/cubestore.rs` (new file)

```rust
use async_trait::async_trait;
use std::sync::Arc;

use crate::{
    transport::{TransportService, LoadRequestMeta, SqlQuery, TransportLoadRequestQuery},
    sql::AuthContextRef,
    compile::MetaContext,
    CubeError,
    cubestore::client::CubeStoreClient,
};
use arrow::record_batch::RecordBatch;

pub struct CubeStoreTransport {
    client: Arc<CubeStoreClient>,
}

impl CubeStoreTransport {
    pub fn new(cubestore_url: String) -> Self {
        Self {
            client: Arc::new(CubeStoreClient::new(cubestore_url)),
        }
    }
}

#[async_trait]
impl TransportService for CubeStoreTransport {
    async fn meta(&self, _ctx: AuthContextRef) -> Result<Arc<MetaContext>, CubeError> {
        // TODO: For prototype, return minimal metadata
        // In full implementation, would fetch from Cube API
        unimplemented!("meta() not implemented in prototype")
    }

    async fn load(
        &self,
        _query: TransportLoadRequestQuery,
        sql_query: Option<SqlQuery>,
        _ctx: AuthContextRef,
        _meta_fields: LoadRequestMeta,
    ) -> Result<Vec<RecordBatch>, CubeError> {
        // Extract SQL string
        let sql = match sql_query {
            Some(SqlQuery::Sql(s)) => s,
            Some(SqlQuery::Query(q)) => q.sql.first().map(|s| s.0.clone()).unwrap_or_default(),
            None => return Err(CubeError::user("No SQL query provided".to_string())),
        };

        // Query CubeStore directly
        self.client.query(sql).await
    }

    // ... other TransportService methods (stub implementations)
}
```

---

## Phase 6: Testing Strategy

### 6.1 Prerequisites

1. **CubeStore running**:
   ```bash
   cd examples/recipes/arrow-ipc
   ./start-cubestore.sh  # Or however you start it locally
   ```

2. **Verify CubeStore accessible**:
   ```bash
   # Using wscat (npm install -g wscat)
   wscat -c ws://localhost:3030/ws
   ```

### 6.2 Test Sequence

**Test 1: Simple Information Schema Query**
```bash
cargo run --example cubestore_direct
```

Expected output:
```
Connecting to CubeStore at ws://127.0.0.1:3030/ws
Executing: SELECT * FROM information_schema.tables LIMIT 5
Results:
  1 batches
  Batch 0: 5 rows × 3 columns
  Schema:
    - table_schema (Utf8)
    - table_name (Utf8)
    - build_range_end (Utf8)
  Data (first 3 rows):
    ...
```

**Test 2: Query Actual Pre-aggregation Table**
```rust
// Modify cubestore_direct.rs
let sql = "SELECT * FROM dev_pre_aggregations.orders_main LIMIT 10";
```

**Test 3: Arrow IPC Output**

Add to example:
```rust
// After getting batches, write to Arrow IPC file
use arrow::ipc::writer::FileWriter;
use std::fs::File;

let file = File::create("/tmp/cubestore_result.arrow")?;
let mut writer = FileWriter::try_new(file, &batches[0].schema())?;

for batch in &batches {
    writer.write(batch)?;
}
writer.finish()?;

println!("Arrow IPC file written to /tmp/cubestore_result.arrow");
```

Then verify with Python:
```python
import pyarrow as pa
import pyarrow.ipc as ipc

with open('/tmp/cubestore_result.arrow', 'rb') as f:
    reader = ipc.open_file(f)
    table = reader.read_all()
    print(table)
```

---

## Phase 7: Error Handling

### 7.1 Error Types to Handle

```rust
impl CubeStoreClient {
    async fn query(&self, sql: String) -> Result<Vec<RecordBatch>, CubeError> {
        // Connection errors
        let (ws_stream, _) = connect_async(&self.url)
            .await
            .map_err(|e| CubeError::internal(format!("WebSocket connection failed: {}", e)))?;

        // Send errors
        write.send(Message::Binary(msg_bytes))
            .await
            .map_err(|e| CubeError::internal(format!("Failed to send query: {}", e)))?;

        // Timeout handling
        let timeout_duration = Duration::from_secs(30);

        tokio::select! {
            msg_result = read.next() => {
                match msg_result {
                    Some(Ok(msg)) => { /* process */ }
                    Some(Err(e)) => return Err(CubeError::internal(format!("WebSocket error: {}", e))),
                    None => return Err(CubeError::internal("Connection closed".to_string())),
                }
            }
            _ = tokio::time::sleep(timeout_duration) => {
                return Err(CubeError::internal("Query timeout".to_string()));
            }
        }
    }
}
```

---

## Configuration

### Environment Variables

```bash
# For standalone example
export CUBESQL_CUBESTORE_URL=ws://localhost:3030/ws
export RUST_LOG=debug

# Run
cargo run --example cubestore_direct
```

---

## Success Criteria

The prototype is successful if:

1. ✅ **Connects to CubeStore**: WebSocket connection established
2. ✅ **Sends Query**: FlatBuffers message sent successfully
3. ✅ **Receives Response**: FlatBuffers response parsed
4. ✅ **Converts to Arrow**: RecordBatch created with correct schema and data
5. ✅ **Arrow IPC Output**: Can write to Arrow IPC file readable by other tools

---

## File Structure

```
rust/cubesql/cubesql/
├── Cargo.toml                           # Updated dependencies
├── src/
│   ├── lib.rs                           # Add: pub mod cubestore;
│   └── cubestore/
│       ├── mod.rs                       # New: module declaration
│       └── client.rs                    # New: ~200 lines
└── examples/
    └── cubestore_direct.rs              # New: ~100 lines

Total new code: ~300 lines
```

---

## Implementation Order

1. ✅ **Check dependencies** in Cargo.toml
2. ✅ **Create cubestore module** (mod.rs, client.rs stub)
3. ✅ **Implement build_query_message()** - FlatBuffers encoding
4. ✅ **Implement query() method** - WebSocket connection & send/receive
5. ✅ **Implement flatbuffers_to_arrow()** - Type inference & conversion
6. ✅ **Create standalone example** - cubestore_direct.rs
7. ✅ **Test with information_schema** query
8. ✅ **Test with pre-aggregation table** query
9. ✅ **Add Arrow IPC file output** to example
10. ✅ **Verify with external tool** (Python/R)

---

## Next Steps After Prototype

Once prototype works:

1. **Integration**: Wire into existing cubesqld query path
2. **Schema Sync**: Fetch metadata from Cube API
3. **Smart Routing**: Decide CubeStore vs Cube API per query
4. **Security**: Inject WHERE clauses from security context
5. **Connection Pooling**: Reuse WebSocket connections
6. **Error Recovery**: Retry logic, fallback to Cube API

---

## Estimated Effort

- **Phase 1-2 (Core client)**: 4-6 hours
- **Phase 3-4 (Integration & example)**: 2-3 hours
- **Phase 5-6 (Testing & debugging)**: 3-4 hours
- **Phase 7 (Error handling & polish)**: 2-3 hours

**Total**: ~1-2 days for working prototype
