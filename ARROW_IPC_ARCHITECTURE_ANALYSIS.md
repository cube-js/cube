# Apache Arrow & Arrow IPC Architecture in Cube

Comprehensive analysis of how Apache Arrow is used in Cube's Rust components and how to enhance Arrow IPC access.

## Table of Contents

1. [Overview: Arrow's Role in Cube](#overview-arrows-role-in-cube)
2. [Current Architecture](#current-architecture)
3. [Arrow in Query Execution](#arrow-in-query-execution)
4. [Current Arrow IPC Implementation](#current-arrow-ipc-implementation)
5. [Data Flow Diagrams](#data-flow-diagrams)
6. [Enhancement: Adding Arrow IPC Access](#enhancement-adding-arrow-ipc-access)
7. [Implementation Roadmap](#implementation-roadmap)

---

## Overview: Arrow's Role in Cube

### Why Apache Arrow in Cube?

Arrow serves as the **universal data format** across Cube's entire system:

1. **Columnar Format**: Efficient for analytical queries (main use case)
2. **Language Neutral**: Work seamlessly with Python, JavaScript, Rust, Java clients
3. **Zero-Copy Access**: RecordBatch can be read without deserialization
4. **Standard IPC Protocol**: Arrow IPC enables interprocess communication with any Arrow-compatible tool
5. **Ecosystem**: Works with Apache Spark, Pandas, Polars, DuckDB, etc.

### Arrow Components in Cube

```
┌─────────────────────────────────────────────────────┐
│                Cube Data Architecture               │
├─────────────────────────────────────────────────────┤
│ Input:     JSON (HTTP) → Arrow RecordBatch          │
│ Storage:   Parquet (Arrow-based) on disk            │
│ Memory:    Vec<RecordBatch> in process memory       │
│ Network:   Arrow IPC Streaming Format               │
│ Output:    PostgreSQL Protocol / JSON / Arrow IPC   │
└─────────────────────────────────────────────────────┘
```

---

## Current Architecture

### Core Components Using Arrow

#### 1. **CubeSQL** - PostgreSQL Protocol Proxy
**Path**: `/rust/cubesql/cubesql/src/`

**Role**: Accepts SQL queries, returns results via PostgreSQL wire protocol

**Arrow Usage**:
```rust
// Query execution pipeline
SQL String
  ↓ (DataFusion Parser)
Logical Plan
  ↓ (DataFusion Optimizer)
Physical Plan
  ↓ (ExecutionPlan)
SendableRecordBatchStream
  ↓ (RecordBatch extraction)
Vec<RecordBatch>
  ↓ (Type conversion)
PostgreSQL Wire Format
```

**Key Files**:
- `sql/postgres/writer.rs` - Convert Arrow arrays to PostgreSQL binary format
- `compile/engine/df/scan.rs` - CubeScan node that fetches data from Cube.js
- `transport/service.rs` - HTTP transport to Cube.js API

#### 2. **CubeStore** - Distributed Columnar Storage
**Path**: `/rust/cubestore/cubestore/src/`

**Role**: Distributed OLAP engine for pre-aggregations and data caching

**Arrow Usage**:
```rust
// Data processing pipeline
SerializedPlan (network message)
  ↓ (Deserialization)
DataFusion ExecutionPlan
  ↓ (Parquet reading + in-memory data)
SendableRecordBatchStream
  ↓ (Local execution)
Vec<RecordBatch>
  ↓ (Arrow IPC serialization)
SerializedRecordBatchStream (network payload)
  ↓ (Network transfer)
Remote Node
  ↓ (Deserialization)
Vec<RecordBatch>
```

**Key Files**:
- `queryplanner/query_executor.rs` - Executes distributed queries
- `table/data.rs` - Row↔Column conversion (Arrow builders/arrays)
- `table/parquet.rs` - Parquet I/O using Arrow reader/writer
- `cluster/message.rs` - Cluster communication with Arrow data

#### 3. **DataFusion** - Query Engine
**Path**: Custom fork at `https://github.com/cube-js/arrow-datafusion`

**Role**: SQL parsing, query planning, physical execution

**Arrow Capabilities**:
- Logical plan optimization
- Physical plan generation
- RecordBatch streaming execution
- Array computation kernels
- Type system (Schema, DataType, Field)

---

## Arrow in Query Execution

### Complete Query Execution Flow

#### **CubeSQL Query Path** (PostgreSQL Client → Cube.js Data)

```
1. Client Connection
   ├─ psql, DBeaver, Python psycopg2, etc.
   └─ PostgreSQL wire protocol

2. SQL Parsing & Planning (CubeSQL)
   ├─ Parse: "SELECT status, SUM(amount) FROM Orders GROUP BY status"
   └─ → DataFusion Logical Plan

3. Plan Optimization
   ├─ Projection pushdown
   ├─ Predicate pushdown
   ├─ Join reordering
   └─ → Optimized Logical Plan

4. Physical Planning
   ├─ CubeScan node (custom DataFusion operator)
   ├─ GroupBy operator
   ├─ Projection operator
   └─ → Physical ExecutionPlan

5. Execution (Arrow RecordBatch streaming)
   ├─ CubeScan::execute()
   │  ├─ Extract member fields from logical plan
   │  └─ Call Cube.js V1Load API with query
   │
   ├─ Cube.js Response
   │  └─ V1LoadResponse (JSON)
   │
   ├─ Convert JSON → Arrow
   │  ├─ Build StringArray for dimensions
   │  ├─ Build Float64Array for measures
   │  └─ Create RecordBatch
   │
   ├─ GroupBy execution
   │  ├─ Hash aggregation over RecordBatch
   │  └─ Output RecordBatch (status, sum(amount))
   │
   └─ Final RecordBatch Stream

6. PostgreSQL Protocol Encoding
   ├─ Extract arrays from RecordBatch
   ├─ Convert each array element to PostgreSQL format
   │  ├─ String → text or bytea
   │  ├─ Int64 → 8-byte big-endian integer
   │  ├─ Float64 → 8-byte IEEE double
   │  └─ Decimal → PostgreSQL numeric format
   └─ Send over wire

7. Client Receives
   └─ Result set formatted as PostgreSQL rows
```

### Arrow Array Types in Cube

**File**: `/rust/cubesql/cubesql/src/sql/postgres/writer.rs`

```rust
// Type conversion for PostgreSQL output
match array_type {
    DataType::String => {
        // StringArray → TEXT or BYTEA
        for value in string_array.iter() {
            write_text_value(value);
        }
    },
    DataType::Int64 => {
        // Int64Array → INT8 (8 bytes)
        for value in int64_array.iter() {
            socket.write_i64(value);
        }
    },
    DataType::Float64 => {
        // Float64Array → FLOAT8
        for value in float64_array.iter() {
            socket.write_f64(value);
        }
    },
    DataType::Decimal128 => {
        // Decimal128Array → NUMERIC
        // Custom encoding for PostgreSQL numeric type
        for value in decimal_array.iter() {
            write_postgres_numeric(value);
        }
    },
    // ... other types ...
}
```

**Supported Arrow Types in Cube**:
- StringArray
- Int16Array, Int32Array, Int64Array
- Float32Array, Float64Array
- BooleanArray
- Decimal128Array
- TimestampArray (various precisions)
- Date32Array, Date64Array
- BinaryArray
- ListArray (for complex types)

---

## Current Arrow IPC Implementation

### Existing Arrow IPC Usage

#### Location: `/rust/cubestore/cubestore/src/queryplanner/query_executor.rs`

**What it does**: Serializes RecordBatch for network transfer between router and worker nodes

```rust
pub struct SerializedRecordBatchStream {
    #[serde(with = "serde_bytes")]  // Efficient binary serialization
    record_batch_file: Vec<u8>,      // Arrow IPC streaming format bytes
}

impl SerializedRecordBatchStream {
    /// Serialize RecordBatches to Arrow IPC format
    pub fn write(
        schema: &Schema,
        record_batches: Vec<RecordBatch>,
    ) -> Result<Vec<Self>, CubeError> {
        let mut results = Vec::with_capacity(record_batches.len());

        for batch in record_batches {
            let file = Vec::new();
            // Create Arrow IPC streaming writer
            let mut writer = MemStreamWriter::try_new(
                Cursor::new(file),
                schema
            )?;

            // Write batch to IPC format
            writer.write(&batch)?;

            // Extract serialized bytes
            let cursor = writer.finish()?;
            results.push(Self {
                record_batch_file: cursor.into_inner(),
            })
        }
        Ok(results)
    }

    /// Deserialize Arrow IPC format back to RecordBatch
    pub fn read(self) -> Result<RecordBatch, CubeError> {
        let cursor = Cursor::new(self.record_batch_file);
        let mut reader = StreamReader::try_new(cursor)?;

        // Read first batch
        let batch = reader.next();
        // ... error handling ...
    }
}
```

### How Arrow IPC Works (Technical Details)

**Arrow IPC Streaming Format** (RFC 0017):

```
Header (metadata):
  ┌─────────────────────────────────────┐
  │ Magic Number (0xFFFFFFFF)           │
  │ Message Type (SCHEMA / RECORD_BATCH)│
  │ Message Length                      │
  │ Message Body (FlatBuffers)          │
  └─────────────────────────────────────┘

Message Body (FlatBuffers):
  ┌─────────────────────────────────────┐
  │ Schema Definition (first message)    │
  │  ├─ Field names                      │
  │  ├─ Data types                       │
  │  └─ Nullability info                 │
  │                                      │
  │ RecordBatch Metadata (per batch)     │
  │  ├─ Number of rows                   │
  │  ├─ Buffer offsets & sizes           │
  │  ├─ Validity bitmap offset           │
  │  ├─ Data buffer offset               │
  │  └─ Nullability counts               │
  └─────────────────────────────────────┘

Data Buffers:
  ┌─────────────────────────────────────┐
  │ Validity Bitmap (nullable columns)   │
  │ Data Buffers (column data)           │
  │  ├─ Column 1 buffer                  │
  │  ├─ Column 2 buffer                  │
  │  └─ ...                              │
  │ Optional Offsets (variable length)   │
  └─────────────────────────────────────┘
```

### Current Network Protocol Using Arrow IPC

**File**: `/rust/cubestore/cubestore/src/cluster/message.rs`

```rust
pub enum NetworkMessage {
    // Streaming SELECT with schema handshake
    SelectStart(SerializedPlan),
    SelectResultSchema(Result<SchemaRef, CubeError>),
    SelectResultBatch(Result<Option<SerializedRecordBatchStream>, CubeError>),

    // In-memory chunk transfer (uses Arrow IPC)
    AddMemoryChunk {
        chunk_name: String,
        data: SerializedRecordBatchStream,
    },
}

// Wire protocol
async fn send_impl(&self, socket: &mut TcpStream) -> Result<(), std::io::Error> {
    let mut ser = flexbuffers::FlexbufferSerializer::new();
    self.serialize(&mut ser).unwrap();
    let message_buffer = ser.take_buffer();
    let len = message_buffer.len() as u64;

    // Write header: Magic (4B) + Version (4B) + Length (8B)
    socket.write_u32(MAGIC).await?;              // 94107
    socket.write_u32(NETWORK_MESSAGE_VERSION).await?;  // 1
    socket.write_u64(len).await?;

    // Write payload (FlexBuffers containing SerializedRecordBatchStream)
    socket.write_all(message_buffer.as_slice()).await?;
}
```

### Storage: Parquet (Arrow-based)

**File**: `/rust/cubestore/cubestore/src/table/parquet.rs`

```rust
pub struct ParquetTableStore {
    // ... config ...
}

impl ParquetTableStore {
    pub fn read_columns(
        &self,
        path: &str
    ) -> Result<Vec<RecordBatch>, CubeError> {
        // Create Parquet reader
        let mut reader = ParquetFileArrowReader::new(
            Arc::new(self.file_reader(path)?)
        );

        // Read into RecordBatches
        let schema = reader.get_schema();
        let batches = reader.get_record_reader(
            1024 * 1024 * 16  // 16MB batch size
        )?
        .collect::<Result<Vec<_>, _>>()?;

        Ok(batches)
    }

    pub fn write_columns(
        &self,
        path: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<(), CubeError> {
        // Create Parquet writer
        let writer = ArrowWriter::try_new(
            File::create(path)?,
            schema,
            Some(WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build()),
        )?;

        for batch in batches {
            writer.write(&batch)?;
        }

        writer.finish()?;
    }
}
```

---

## Data Flow Diagrams

### Diagram 1: Query Execution (High Level)

```
Client (psql/DBeaver/Python)
    ↓ (PostgreSQL wire protocol)
    │
CubeSQL Server
    ├─ Parse SQL → Logical Plan (DataFusion)
    ├─ Optimize → Physical Plan
    ├─ Plan → CubeScan node
    │
    ├─ CubeScan::execute()
    │  ├─ Extract dimensions, measures
    │  └─ Call Cube.js API (REST/JSON)
    │
    ├─ Cube.js Response (JSON)
    │  └─ V1LoadResponse { data: [...], }
    │
    ├─ Convert JSON → Arrow RecordBatch
    │  ├─ Build ArrayRef for each column
    │  ├─ StringArray, Float64Array, etc.
    │  └─ RecordBatch { schema, columns, row_count }
    │
    ├─ Execute remaining operators
    │  └─ GroupBy, Filter, Sort, etc. (on RecordBatch)
    │
    ├─ Output RecordBatch
    │  └─ Final result set
    │
    ├─ Convert to PostgreSQL Protocol
    │  ├─ Extract arrays
    │  ├─ For each value: encode to binary
    │  └─ Send via TCP socket
    │
    └─ Client receives rows
```

### Diagram 2: Distributed Execution (CubeStore)

```
Router Node
    │
    ├─ Parse SerializedPlan (from cluster message)
    ├─ Create ExecutionPlan with distributed operators
    │
    ├─ Send subqueries to Worker nodes
    │  └─ Via NetworkMessage::SelectStart(plan)
    │
    ├─ Receive Worker responses
    │  ├─ SelectResultSchema (Arrow Schema)
    │  └─ SelectResultBatch (SerializedRecordBatchStream)
    │       └─ Arrow IPC bytes → RecordBatch
    │
    ├─ Merge partial results
    │  └─ Union + GroupBy on merged batches
    │
    └─ Return final RecordBatch


Worker Node
    │
    ├─ Receive SerializedPlan
    ├─ Create ExecutionPlan
    │
    ├─ Fetch data
    │  ├─ Read Parquet files (Arrow reader)
    │  │  └─ Parquet bytes → RecordBatch (via Arrow)
    │  ├─ Query in-memory chunks
    │  │  └─ Vec<RecordBatch> from HashMap
    │  └─ Combine sources
    │
    ├─ Execute local operators
    │  └─ Scan → Filter → Aggregation → Project
    │
    ├─ Serialize output
    │  └─ RecordBatch → Arrow IPC bytes
    │
    └─ Send back to Router
       └─ Via SerializedRecordBatchStream
```

### Diagram 3: Data Format Transformations

```
HTTP/REST (from Cube.js)
    ↓ (JSON)
    │
Application Code (JSON parsing)
    ├─ Deserialize V1LoadResponse
    ├─ Extract row data
    └─ Call array builders
    │
Arrow Array Builders (accumulate values)
    ├─ StringArrayBuilder.append_value()
    ├─ Float64ArrayBuilder.append_value()
    └─ ...
    │
Array Finish
    ├─ ArrayRef (Arc<dyn Array>)
    ├─ StringArray, Float64Array, etc.
    └─ Build complete arrays
    │
RecordBatch Creation
    ├─ RecordBatch { schema, columns: Vec<ArrayRef>, row_count }
    └─ In-memory columnar representation
    │
Serialization Paths (from RecordBatch):
    │
    ├─ Path A: Arrow IPC
    │  ├─ MemStreamWriter
    │  ├─ Write schema (FlatBuffer message)
    │  ├─ Write batches (FlatBuffer + data buffers)
    │  └─ Vec<u8> (Arrow IPC bytes)
    │
    ├─ Path B: Parquet
    │  ├─ ArrowWriter
    │  ├─ Compress columns
    │  ├─ Write metadata
    │  └─ .parquet file
    │
    └─ Path C: PostgreSQL Protocol
       ├─ Extract arrays
       ├─ For each column/row, encode type-specific format
       └─ Binary wire format
```

---

## Enhancement: Adding Arrow IPC Access

### Current Limitation

**What's missing**: Direct Arrow IPC endpoint for clients to retrieve data in Arrow IPC format

**Why it matters**:
- Arrow IPC is zero-copy (no parsing overhead)
- Compatible with Arrow libraries in Python, R, Java, C++, Node.js
- Can be memory-mapped directly
- Streaming support for large datasets
- Standard Apache Arrow format

### Proposed Enhancement Architecture

#### **Option 1: Arrow IPC Output Mode (Recommended for Quick Implementation)**

Add an output format option to CubeSQL for Arrow IPC instead of PostgreSQL protocol:

```rust
// New enum for output formats
pub enum OutputFormat {
    PostgreSQL,      // Current: PostgreSQL wire protocol
    ArrowIPC,        // New: Arrow IPC streaming format
    JSON,            // Alternative: JSON
    Parquet,         // Alternative: Parquet file
}

// Connection configuration
pub struct SessionConfig {
    output_format: OutputFormat,
    // ... other settings ...
}

// Usage in response handler
match session.output_format {
    OutputFormat::PostgreSQL => {
        // Existing code
        encode_postgres_protocol(&record_batch, socket)
    },
    OutputFormat::ArrowIPC => {
        // New code
        encode_arrow_ipc(&record_batch, socket)
    },
}
```

**Implementation Requirements**:

1. **Query Parameter or Connection Option**
   ```sql
   -- Option A: Connection string
   postgresql://host:5432/?output_format=arrow_ipc

   -- Option B: SET command
   SET output_format = 'arrow_ipc';

   -- Option C: Custom SQL dialect
   SELECT * FROM table FORMAT arrow_ipc;
   ```

2. **Handler Function**
   ```rust
   async fn handle_arrow_ipc_query(
       session: &mut Session,
       query: &str,
       socket: &mut TcpStream,
   ) -> Result<(), Error> {
       // Parse and execute query
       let record_batches = execute_query(query).await?;

       // Serialize to Arrow IPC
       let ipc_bytes = serialize_to_arrow_ipc(&record_batches)?;

       // Send to client
       socket.write_all(&ipc_bytes).await?;
       Ok(())
   }

   fn serialize_to_arrow_ipc(batches: &[RecordBatch]) -> Result<Vec<u8>> {
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
   ```

3. **Client Library (Python Example)**
   ```python
   import pyarrow as pa
   import socket

   # Connect and execute query
   sock = socket.socket()
   sock.connect(("localhost", 5432))

   # Send Arrow IPC query request
   request = b"SELECT * FROM orders FORMAT arrow_ipc"
   sock.send(request)

   # Receive Arrow IPC bytes
   data = sock.recv(1000000)

   # Parse with Arrow
   reader = pa.RecordBatchStreamReader(data)
   table = reader.read_all()

   # Work with Arrow Table
   print(table.to_pandas())
   ```

#### **Option 2: Dedicated Arrow IPC Service (More Comprehensive)**

Create a separate service endpoint specifically for Arrow IPC:

```rust
// New service alongside CubeSQL
pub struct ArrowIPCService {
    cube_sql: Arc<CubeSQL>,
    listen_addr: SocketAddr,
}

impl ArrowIPCService {
    pub async fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(self.listen_addr).await?;

        loop {
            let (socket, _) = listener.accept().await?;
            let cube_sql = self.cube_sql.clone();

            tokio::spawn(async move {
                handle_arrow_ipc_client(socket, cube_sql).await
            });
        }
    }
}

async fn handle_arrow_ipc_client(
    mut socket: TcpStream,
    cube_sql: Arc<CubeSQL>,
) -> Result<()> {
    // Custom Arrow IPC query protocol
    loop {
        // Read query length
        let len = socket.read_u32().await? as usize;

        // Read query string
        let mut query_bytes = vec![0u8; len];
        socket.read_exact(&mut query_bytes).await?;
        let query = String::from_utf8(query_bytes)?;

        // Execute query
        let record_batches = cube_sql.execute(&query).await?;

        // Serialize to Arrow IPC
        let output = Vec::new();
        let mut writer = MemStreamWriter::try_new(
            Cursor::new(output),
            &record_batches[0].schema(),
        )?;

        for batch in &record_batches {
            writer.write(batch)?;
        }

        let ipc_data = writer.finish()?.into_inner();

        // Send back: length + IPC data
        socket.write_u32(ipc_data.len() as u32).await?;
        socket.write_all(&ipc_data).await?;
    }
}
```

#### **Option 3: HTTP REST Endpoint (For Web Clients)**

Expose Arrow IPC over HTTP:

```rust
// New HTTP endpoint
pub async fn arrow_ipc_query(
    Query(params): Query<ArrowQueryParams>,
) -> Result<impl Response> {
    let query = params.sql;

    // Execute query
    let record_batches = execute_query(&query).await?;

    // Serialize to Arrow IPC
    let ipc_bytes = serialize_to_arrow_ipc(&record_batches)?;

    // Return as application/x-arrow-ipc content type
    Ok(HttpResponse::Ok()
        .content_type("application/x-arrow-ipc")
        .body(ipc_bytes))
}

// Client usage
fetch('/api/arrow-ipc?sql=SELECT * FROM orders')
    .then(r => r.arrayBuffer())
    .then(buffer => {
        const reader = arrow.RecordBatchStreamReader(buffer);
        const table = reader.readAll();
    });
```

### Implementation Steps

#### **Phase 1: Basic Arrow IPC Output (Week 1)**

1. **Add OutputFormat enum** to session configuration
2. **Implement serialize_to_arrow_ipc()** function
3. **Add format handling** in response dispatcher
4. **Test** with PyArrow client

**Files to Modify**:
- `rust/cubesql/cubesql/src/server/session.rs` - Add output format
- `rust/cubesql/cubesql/src/sql/response.rs` - Add formatter
- Create `rust/cubesql/cubesql/src/sql/arrow_ipc.rs` - New serializer

#### **Phase 2: Query Parameter Support (Week 2)**

1. **Parse output format parameter** from connection string
2. **Add SET command** support for output format
3. **Handle streaming** for large result sets
4. **Add unit tests** for serialization

**Files to Modify**:
- `rust/cubesql/cubesql/src/server/connection.rs` - Parse parameters
- `rust/cubesql/cubesql/src/sql/ast.rs` - Extend AST for SET commands
- Add integration tests

#### **Phase 3: Client Libraries & Examples (Week 3)**

1. **Python client example** using PyArrow
2. **JavaScript/Node.js client** using Apache Arrow JS
3. **R client example** using Arrow R package
4. **Documentation** and tutorials

**Create**:
- `examples/arrow-ipc-client-python.py`
- `examples/arrow-ipc-client-js.js`
- `examples/arrow-ipc-client-r.R`
- `docs/arrow-ipc-guide.md`

#### **Phase 4: Advanced Features (Week 4)**

1. **Streaming support** for large datasets
2. **Compression support** (with Arrow codec)
3. **Schema evolution** handling
4. **Performance optimization** (zero-copy buffers)

**Enhancements**:
- `SerializedRecordBatchStream` with streaming
- Compression middleware
- Memory-mapped buffer support

### Code Example: Complete Implementation

```rust
// File: rust/cubesql/cubesql/src/sql/arrow_ipc.rs

use datafusion::arrow::ipc::writer::MemStreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use std::io::Cursor;

pub struct ArrowIPCSerializer;

impl ArrowIPCSerializer {
    /// Serialize RecordBatches to Arrow IPC Streaming Format
    pub fn serialize_streaming(
        batches: &[RecordBatch],
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        if batches.is_empty() {
            return Ok(Vec::new());
        }

        let schema = batches[0].schema();
        let mut output = Vec::new();
        let cursor = Cursor::new(&mut output);

        let mut writer = MemStreamWriter::try_new(cursor, schema)?;

        // Write all batches
        for batch in batches {
            writer.write(batch)?;
        }

        // Finalize and extract buffer
        let cursor = writer.finish()?;
        Ok(cursor.into_inner().clone())
    }

    /// Serialize with streaming (for large datasets)
    pub fn serialize_streaming_iter<'a>(
        batches: impl Iterator<Item = &'a RecordBatch>,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut output = Vec::new();
        let mut writer: Option<MemStreamWriter<Cursor<&mut Vec<u8>>>> = None;

        for batch in batches {
            if writer.is_none() {
                let cursor = Cursor::new(&mut output);
                writer = Some(MemStreamWriter::try_new(cursor, batch.schema())?);
            }

            if let Some(ref mut w) = writer {
                w.write(batch)?;
            }
        }

        if let Some(w) = writer {
            w.finish()?;
        }

        Ok(output)
    }
}

// File: rust/cubesql/cubesql/src/server/session.rs

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OutputFormat {
    PostgreSQL,  // Default: PostgreSQL wire protocol
    ArrowIPC,    // New: Arrow IPC streaming format
    JSON,        // Alternative
}

pub struct Session {
    // ... existing fields ...
    pub output_format: OutputFormat,
}

impl Session {
    pub fn new(output_format: OutputFormat) -> Self {
        Self {
            output_format,
            // ... other initialization ...
        }
    }
}

// File: rust/cubesql/cubesql/src/sql/response.rs

pub async fn handle_query_response(
    session: &Session,
    record_batches: Vec<RecordBatch>,
    socket: &mut TcpStream,
) -> Result<()> {
    match session.output_format {
        OutputFormat::PostgreSQL => {
            // Existing code
            encode_postgres_protocol(&record_batches, socket).await
        }
        OutputFormat::ArrowIPC => {
            // New code
            let ipc_bytes = ArrowIPCSerializer::serialize_streaming(&record_batches)?;

            // Send length header
            socket.write_u32(ipc_bytes.len() as u32).await?;

            // Send IPC data
            socket.write_all(&ipc_bytes).await?;

            Ok(())
        }
        OutputFormat::JSON => {
            // Existing or new code
            encode_json(&record_batches, socket).await
        }
    }
}
```

### Testing Strategy

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::*;
    use datafusion::arrow::datatypes::*;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::arrow::ipc::reader::StreamReader;
    use std::io::Cursor;

    #[test]
    fn test_arrow_ipc_roundtrip() {
        // Create test RecordBatch
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, false),
        ]));

        let names = Arc::new(StringArray::from(vec!["Alice", "Bob"]));
        let ages = Arc::new(Int32Array::from(vec![25, 30]));

        let batch = RecordBatch::try_new(schema.clone(), vec![names, ages]).unwrap();

        // Serialize to Arrow IPC
        let ipc_bytes = ArrowIPCSerializer::serialize_streaming(&[batch.clone()]).unwrap();

        // Deserialize from Arrow IPC
        let reader = StreamReader::try_new(Cursor::new(ipc_bytes)).unwrap();
        let batches: Vec<_> = reader.collect::<Result<_, _>>().unwrap();

        // Verify
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].schema(), batch.schema());
        assert_eq!(batches[0].num_rows(), batch.num_rows());
    }
}
```

---

## Implementation Roadmap

### Timeline & Effort Estimate

| Phase | Focus | Duration | Effort |
|-------|-------|----------|--------|
| **1** | Basic Arrow IPC output | 1 week | 20 hours |
| **2** | Connection parameters | 1 week | 15 hours |
| **3** | Client libraries | 1 week | 25 hours |
| **4** | Advanced features | 2 weeks | 30 hours |
| **Total** | Complete implementation | 5 weeks | 90 hours |

### Dependency Graph

```
Phase 1 (Basic Serialization)
    ↓
Phase 2 (Query Parameters) ← depends on Phase 1
    ↓
Phase 3 (Client Libraries) ← depends on Phase 1
    ↓
Phase 4 (Optimization) ← depends on Phase 1, 2, 3
```

### Success Criteria

- ✅ Arrow IPC serialization works for all Arrow data types
- ✅ Query parameters correctly configure output format
- ✅ Clients can receive and parse Arrow IPC format
- ✅ Performance: streaming support for 1GB+ datasets
- ✅ Compatibility: works with PyArrow, Arrow JS, Arrow R
- ✅ Documentation: complete guide and examples

### Testing Requirements

| Test Type | Coverage | Priority |
|-----------|----------|----------|
| Unit Tests | Serialization/deserialization | High |
| Integration Tests | End-to-end queries | High |
| Performance Tests | Large datasets (>1GB) | Medium |
| Client Tests | Python, JS, R clients | High |
| Compatibility Tests | Various Arrow versions | Medium |

---

## Key Considerations

### 1. **Backward Compatibility**
- Arrow IPC output must be optional (default to PostgreSQL)
- Existing clients must continue working
- Connection string parsing must be non-breaking

### 2. **Performance**
- Arrow IPC should be faster than PostgreSQL protocol encoding
- Benchmark: PostgreSQL vs Arrow IPC serialization time
- Use streaming for large result sets

### 3. **Security**
- Arrow IPC still requires authentication
- Data is not encrypted by default (use TLS)
- Same permissions model as PostgreSQL

### 4. **Compatibility**
- Support multiple Arrow versions
- Handle schema evolution gracefully
- Work with Arrow libraries in all languages

### 5. **Documentation**
- Tutorial: "Getting Started with Arrow IPC"
- API reference for output formats
- Performance comparison guide
- Example applications

---

## Conclusion

Apache Arrow is already deeply integrated into Cube's architecture as the universal data format. Enhancing Arrow IPC access would:

1. **Enable efficient client access** to data in native Arrow format
2. **Reduce latency** by eliminating format conversions
3. **Improve compatibility** with Arrow ecosystem tools
4. **Maintain backward compatibility** with existing PostgreSQL clients
5. **Support streaming** for large datasets

The implementation is straightforward given existing Arrow serialization in CubeStore, and would provide significant value to data science and analytics workflows.
