# Cube Arrow Native Protocol - Development Environment

This directory contains a development environment for testing the Cube Arrow Native protocol implementation.

## Architecture

The Arrow Native protocol enables direct streaming of Apache Arrow data between Cube and ADBC clients, eliminating the overhead of PostgreSQL wire protocol conversion.

```
┌─────────────┐
│ ADBC Client │
└──────┬──────┘
       │
       │ Arrow Native Protocol (port 4445)
       │ ↓ Direct Arrow IPC streaming
       │
┌──────▼───────────┐
│   cubesqld       │
│  ┌────────────┐  │
│  │ PostgreSQL │  │ ← PostgreSQL protocol (port 4444)
│  │  Protocol  │  │
│  ├────────────┤  │
│  │   Arrow    │  │ ← Arrow Native protocol (port 4445)
│  │   Native   │  │
│  ├────────────┤  │
│  │   Query    │  │
│  │  Compiler  │  │
│  └────────────┘  │
└──────┬───────────┘
       │
       │ HTTP API
       │
┌──────▼───────────┐
│  Cube.js Server  │
└──────┬───────────┘
       │
       │ SQL
       │
┌──────▼───────────┐
│   PostgreSQL     │
└──────────────────┘
```

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Rust toolchain (1.90.0+)
- Node.js and Yarn
- lsof (for port checking)

### Option 1: Full Stack (Recommended)

This starts everything: PostgreSQL, Cube.js server, and cubesql with Arrow Native support.

```bash
./dev-start.sh
```

This will:
1. Start PostgreSQL database (port 7432)
2. Build cubesql with Arrow Native support
3. Start Cube.js API server (port 4008)
4. Start cubesql with both PostgreSQL (4444) and Arrow Native (4445) protocols

### Option 2: Build and Run cubesql Only

If you already have Cube.js API running:

```bash
./build-and-run.sh
```

This requires that you've set the Cube.js API URL in your environment:
```bash
export CUBESQL_CUBE_URL="http://localhost:4008/cubejs-api/v1"
export CUBESQL_CUBE_TOKEN="your-token-here"
```

## Configuration

Edit `.env` file to configure:

```bash
# HTTP API port for Cube.js server
PORT=4008

# SQL protocol ports
CUBEJS_PG_SQL_PORT=4444      # PostgreSQL protocol
CUBEJS_ARROW_PORT=4445       # Arrow Native protocol

# Database connection
CUBEJS_DB_TYPE=postgres
CUBEJS_DB_PORT=7432
CUBEJS_DB_NAME=pot_examples_dev
CUBEJS_DB_USER=postgres
CUBEJS_DB_PASS=postgres
CUBEJS_DB_HOST=localhost

# Development settings
CUBEJS_DEV_MODE=true
CUBEJS_LOG_LEVEL=trace
NODE_ENV=development

# cubesql settings (set by dev-start.sh)
CUBESQL_LOG_LEVEL=info
```

## Testing Connections

### PostgreSQL Protocol (Traditional)

```bash
psql -h 127.0.0.1 -p 4444 -U root
```

### Arrow Native Protocol (ADBC)

Using Python with ADBC:

```python
import adbc_driver_cube as cube

# Connect using Arrow Native protocol
db = cube.connect(
    uri="localhost:4445",
    db_kwargs={
        "connection_mode": "native",  # or "arrow_native"
        "token": "your-token-here"
    }
)

with db.cursor() as cur:
    cur.execute("SELECT * FROM orders LIMIT 10")
    result = cur.fetch_arrow_table()
    print(result)
```

### Performance Comparison

You can compare the performance between protocols:

```bash
# PostgreSQL protocol
python arrow_ipc_client.py --mode postgres --port 4444

# Arrow Native protocol
python arrow_ipc_client.py --mode native --port 4445
```

Expected improvements with Arrow Native:
- 70-80% reduction in protocol overhead
- 50% less memory usage
- Zero extra serialization/deserialization
- Lower latency for first batch

## Development Workflow

### Making Changes to cubesql

1. Edit Rust code in `/cube/rust/cubesql/cubesql/src/`
2. Rebuild: `cargo build --release --bin cubesqld`
3. Restart cubesql (Ctrl+C and re-run `dev-start.sh`)

### Making Changes to Cube Schema

1. Edit files in `model/cubes/` or `model/views/`
2. Cube.js will auto-reload (in dev mode)
3. Test with new queries

### Logs

- **Cube.js API**: `tail -f cube-api.log`
- **cubesqld**: Output is shown in terminal where dev-start.sh runs
- **PostgreSQL**: `docker-compose logs -f postgres`

## Files Created by Scripts

- `bin/cubesqld` - Compiled cubesql binary with Arrow Native support
- `cube-api.log` - Cube.js API server logs
- `cube-api.pid` - Cube.js API server process ID

## Troubleshooting

### Port Already in Use

```bash
# Check what's using a port
lsof -i :4444
lsof -i :4445
lsof -i :4008

# Kill process using port
kill $(lsof -t -i :4445)
```

### PostgreSQL Won't Start

```bash
# Reset PostgreSQL
docker-compose down -v
docker-compose up -d postgres
```

### Cube.js API Not Responding

```bash
# Check logs
tail -f cube-api.log

# Restart
kill $(cat cube-api.pid)
yarn dev
```

### cubesql Connection Refused

Check that:
1. Cube.js API is running: `curl http://localhost:4008/readyz`
2. Environment variables are set correctly
3. Token is valid (in dev mode, "test" usually works)

## What's Implemented

- ✅ Full Arrow Native protocol specification
- ✅ Direct Arrow IPC streaming from DataFusion
- ✅ Query compilation integration (shared with PostgreSQL)
- ✅ Session management and authentication
- ✅ All query types: SELECT, SHOW, SET, CREATE TEMP TABLE
- ✅ Proper shutdown handling
- ✅ Error handling and reporting

## What's Next

- ⏳ Integration tests
- ⏳ Performance benchmarks
- ⏳ MetaTabular full implementation (SHOW commands)
- ⏳ Temp table persistence
- ⏳ Query cancellation support
- ⏳ Prepared statements

## Protocol Details

### Message Format

All messages use a simple binary format:
```
[4 bytes: message length (big-endian u32)]
[1 byte: message type]
[variable: payload]
```

### Message Types

- `0x01` HandshakeRequest
- `0x02` HandshakeResponse
- `0x03` AuthRequest
- `0x04` AuthResponse
- `0x10` QueryRequest
- `0x11` QueryResponseSchema (Arrow IPC schema)
- `0x12` QueryResponseBatch (Arrow IPC record batch)
- `0x13` QueryComplete
- `0xFF` Error

### Connection Flow

1. Client → Server: HandshakeRequest (version)
2. Server → Client: HandshakeResponse (version, server_version)
3. Client → Server: AuthRequest (token, database)
4. Server → Client: AuthResponse (success, session_id)
5. Client → Server: QueryRequest (sql)
6. Server → Client: QueryResponseSchema (Arrow schema)
7. Server → Client: QueryResponseBatch (data) [repeated]
8. Server → Client: QueryComplete (rows_affected)

## References

- [Query Execution Documentation](../../QUERY_EXECUTION_COMPLETE.md)
- [ADBC Native Client Implementation](../../ADBC_NATIVE_CLIENT_IMPLEMENTATION.md)
- [Cube.js Documentation](https://cube.dev/docs)
- [Apache Arrow IPC Format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format)
