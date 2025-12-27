# Cube.js Architecture: Component Orchestration on Single Node

## Overview

This document explains how Cube.js orchestrates CubeStore and CubeSQL when starting the server on a single node.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                      Cube.js Server Process                      │
│                                                                   │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │           Node.js Layer (cubejs-server-core)               │ │
│  │                                                              │ │
│  │  1. API Gateway (Express/HTTP)                              │ │
│  │  2. Query Orchestrator                                      │ │
│  │  3. Schema Compiler                                         │ │
│  └────────────┬─────────────────────────┬─────────────────────┘ │
│               │                          │                        │
│               ├──────────────────────────┤                        │
│               ▼                          ▼                        │
│  ┌────────────────────────┐  ┌──────────────────────┐          │
│  │   SQL Interface        │  │  CubeStore Driver    │          │
│  │  (Rust via N-API)      │  │  (WebSocket Client)  │          │
│  │                        │  │                      │          │
│  │  • CubeSQL Engine      │  │  Host: 127.0.0.1    │          │
│  │  • PostgreSQL Wire     │  │  Port: 3030         │          │
│  │  • ADBC(Arrow Native) Protocol  │  │  Protocol: WS       │          │
│  │  • Port: 5432 (pg)     │  │                      │          │
│  │  • Port: 8120 (arrow)  │  │                      │          │
│  └────────────┬───────────┘  └──────────┬───────────┘          │
│               │                          │                        │
└───────────────┼──────────────────────────┼────────────────────────┘
                │                          │
                ▼                          ▼
      ┌─────────────────┐      ┌────────────────────┐
      │  CubeSQL Binary │      │ CubeStore Process  │
      │  (Embedded Rust)│      │  (External/Dev)    │
      │                 │      │                    │
      │  Same process   │      │  • OLAP Engine     │
      │  as Node.js     │      │  • Pre-agg Storage │
      │                 │      │  • Port: 3030      │
      └─────────────────┘      └────────────────────┘
```

## Component Details

### 1. CubeSQL (Embedded Rust, In-Process)

**Key Characteristics:**
- **Type**: Native Node.js addon via N-API
- **Location**: Embedded in the same process as Node.js
- **Binary**: `packages/cubejs-backend-native/index.node`
- **Startup**: Automatic when Cube.js starts

**Protocols Supported:**
- PostgreSQL wire protocol (port 5432 by default)
- ADBC(Arrow Native) protocol (port 8120)

**Code Reference:**

Location: `packages/cubejs-backend-native/js/index.ts:374`

```typescript
export const registerInterface = async (
  options: SQLInterfaceOptions
): Promise<SqlInterfaceInstance> => {
  const native = loadNative(); // Load Rust binary (index.node)
  return native.registerInterface({
    pgPort: options.pgSqlPort,        // PostgreSQL wire protocol port
    gatewayPort: options.gatewayPort, // ADBC(Arrow Native) port
    contextToApiScopes: ...,
    checkAuth: ...,
    checkSqlAuth: ...,
    load: ...,
    meta: ...,
    stream: ...,
    sqlApiLoad: ...,
    // ... other callback functions
  });
};
```

**Initialization:**

Location: `packages/cubejs-api-gateway/src/sql-server.ts:115`

```typescript
export class SQLServer {
  public async init(options: SQLServerOptions): Promise<void> {
    this.sqlInterfaceInstance = await registerInterface({
      gatewayPort: this.gatewayPort,
      pgPort: options.pgSqlPort,
      contextToApiScopes: async ({ securityContext }) => ...,
      checkAuth: async ({ request, token }) => ...,
      checkSqlAuth: async ({ request, user, password }) => ...,
      load: async ({ request, session, query }) => ...,
      sqlApiLoad: async ({ request, session, query, ... }) => ...,
      // ... more callbacks
    });
  }
}
```

### 2. CubeStore (External Process or Dev Embedded)

**Key Characteristics:**
- **Type**: Separate process (Rust binary)
- **Connection**: WebSocket client from Node.js
- **Default endpoint**: `ws://127.0.0.1:3030/ws`
- **Startup**: Must be started separately (or via dev mode)

**Code Reference:**

Location: `packages/cubejs-cubestore-driver/src/CubeStoreDriver.ts:61-76`

```typescript
export class CubeStoreDriver extends BaseDriver {
  protected readonly connection: WebSocketConnection;

  public constructor(config?: Partial<ConnectionConfig>) {
    super();

    this.config = {
      host: config?.host || getEnv('cubeStoreHost') || '127.0.0.1',
      port: config?.port || getEnv('cubeStorePort') || '3030',
      user: config?.user || getEnv('cubeStoreUser'),
      password: config?.password || getEnv('cubeStorePass'),
    };

    this.baseUrl = (this.config.url || `ws://${this.config.host}:${this.config.port}/`)
      .replace(/\/ws$/, '').replace(/\/$/, '');

    // WebSocket connection to CubeStore
    this.connection = new WebSocketConnection(`${this.baseUrl}/ws`);
  }

  public async query<R = any>(query: string, values: any[]): Promise<R[]> {
    const sql = formatSql(query, values || []);
    return this.connection.query(sql, [], { instance: getEnv('instanceId') });
  }
}
```

### 3. Query Orchestrator Integration

**Code Reference:**

Location: `packages/cubejs-query-orchestrator/src/orchestrator/QueryOrchestrator.ts:90`

```typescript
export class QueryOrchestrator {
  constructor(options) {
    const { cacheAndQueueDriver } = options;

    const cubeStoreDriverFactory = cacheAndQueueDriver === 'cubestore'
      ? async () => {
          if (externalDriverFactory) {
            const externalDriver = await externalDriverFactory();
            if (externalDriver instanceof CubeStoreDriver) {
              return externalDriver;
            }
            throw new Error(
              'It`s not possible to use Cube Store as queue/cache driver ' +
              'without using it as external'
            );
          }
          throw new Error(
            'Cube Store was specified as queue/cache driver. ' +
            'Please set CUBEJS_CUBESTORE_HOST and CUBEJS_CUBESTORE_PORT variables.'
          );
        }
      : undefined;

    this.queryCache = new QueryCache(
      this.redisPrefix,
      driverFactory,
      this.logger,
      {
        externalDriverFactory,
        cacheAndQueueDriver,
        cubeStoreDriverFactory,
        // ...
      }
    );
  }
}
```

## Startup Sequences

### Development Mode (Automatic CubeStore)

```bash
# Cube.js dev server attempts to start CubeStore automatically
npm run dev

# or
yarn dev
```

**What happens:**
1. Cube.js starts Node.js process
2. CubeSQL registers via `registerInterface()` (embedded Rust)
3. Dev server attempts to spawn CubeStore process
4. CubeStore Driver connects to `ws://127.0.0.1:3030/ws`

### Production Mode (Manual CubeStore)

```bash
# Terminal 1: Start CubeStore
cd rust/cubestore
cargo run --release -- --port 3030

# Terminal 2: Start Cube.js
export CUBEJS_CUBESTORE_HOST=127.0.0.1
export CUBEJS_CUBESTORE_PORT=3030
export CUBEJS_PG_SQL_PORT=5432
export CUBEJS_ADBC_PORT=8120
npm start
```

**What happens:**
1. CubeStore starts as separate Rust process on port 3030
2. Cube.js starts Node.js process
3. CubeSQL registers via `registerInterface()` (embedded)
4. CubeStore Driver connects to running CubeStore via WebSocket

### Docker Compose Configuration

```yaml
version: '3'
services:
  cubestore:
    image: cubejs/cubestore:latest
    ports:
      - "3030:3030"
    environment:
      - CUBESTORE_SERVER_NAME=cubestore:3030
      - CUBESTORE_META_PORT=9999
      - CUBESTORE_WORKERS=4
    volumes:
      - cubestore-data:/cube/data

  cube:
    image: cubejs/cube:latest
    depends_on:
      - cubestore
    ports:
      - "4000:4000"  # HTTP API
      - "5432:5432"  # PostgreSQL wire protocol (CubeSQL)
      - "8120:8120"  # ADBC(Arrow Native) (CubeSQL)
    environment:
      # CubeStore connection
      - CUBEJS_CUBESTORE_HOST=cubestore
      - CUBEJS_CUBESTORE_PORT=3030

      # CubeSQL ports
      - CUBEJS_PG_SQL_PORT=5432
      - CUBEJS_ADBC_PORT=8120

      # Use CubeStore for cache/queue
      - CUBEJS_CACHE_AND_QUEUE_DRIVER=cubestore

      # Your data source
      - CUBEJS_DB_TYPE=postgres
      - CUBEJS_DB_HOST=postgres
      - CUBEJS_DB_PORT=5432
    volumes:
      - ./schema:/cube/conf/schema

volumes:
  cubestore-data:
```

## Environment Variables Reference

### CubeStore Connection

```bash
# CubeStore host (default: 127.0.0.1)
CUBEJS_CUBESTORE_HOST=127.0.0.1

# CubeStore port (default: 3030)
CUBEJS_CUBESTORE_PORT=3030

# CubeStore authentication (optional)
CUBEJS_CUBESTORE_USER=
CUBEJS_CUBESTORE_PASS=
```

### CubeSQL Configuration

```bash
# PostgreSQL wire protocol port (default: 5432)
CUBEJS_PG_SQL_PORT=5432

# ADBC(Arrow Native) protocol port (default: 8120)
CUBEJS_ADBC_PORT=8120

# Legacy variable (deprecated, use CUBEJS_ADBC_PORT)
# CUBEJS_SQL_PORT=4445

# Enable/disable SQL API
CUBEJS_SQL_API=true
```

### Cache and Queue Driver

```bash
# Options: 'memory' or 'cubestore'
CUBEJS_CACHE_AND_QUEUE_DRIVER=cubestore

# External pre-aggregations driver
# If using CubeStore for cache, it must be external driver too
CUBEJS_EXTERNAL_DEFAULT=cubestore
```

## Port Usage Summary

| Port | Service | Protocol | Purpose |
|------|---------|----------|---------|
| 4000 | Cube.js | HTTP/REST | REST API, GraphQL |
| 5432 | CubeSQL | PostgreSQL Wire | SQL queries via PostgreSQL protocol |
| 8120 | CubeSQL | ADBC(Arrow Native)/ADBC | ADBC access - Cube.js as ADBC data source (like SQLite, DuckDB, PostgreSQL, Snowflake) |
| 3030 | CubeStore | WebSocket | Pre-aggregation storage, cache, queue |

## Process Architecture

### Single Node Deployment

```
┌─────────────────────────────────────┐
│  Host Machine / Container           │
│                                     │
│  ┌───────────────────────────────┐ │
│  │  Process 1: Node.js           │ │
│  │  ├─ Cube.js Server            │ │
│  │  ├─ CubeSQL (embedded Rust)   │ │
│  │  └─ Ports: 4000, 5432, 8120   │ │
│  └───────────────┬───────────────┘ │
│                  │ WebSocket        │
│                  ▼                  │
│  ┌───────────────────────────────┐ │
│  │  Process 2: CubeStore (Rust)  │ │
│  │  └─ Port: 3030                │ │
│  └───────────────────────────────┘ │
└─────────────────────────────────────┘
```

### Key Insights

1. **CubeSQL is NOT a separate process**
   - It's a Rust library loaded via N-API
   - Runs in the same process as Node.js
   - No IPC overhead for Node.js ↔ CubeSQL communication

2. **CubeStore IS a separate process**
   - Standalone Rust binary
   - Communicates via WebSocket
   - Can be on same or different machine

3. **Connection Flow**
   ```
   Client → CubeSQL (port 5432/8120) → Node.js → CubeStore (port 3030) → Data
   ```

4. **Binary Locations**
   - CubeSQL: `packages/cubejs-backend-native/index.node`
   - CubeStore: `rust/cubestore/target/release/cubesqld` (or Docker image)

## Debugging and Troubleshooting

### Check if CubeSQL is running

```bash
# PostgreSQL protocol
psql -h localhost -p 5432 -U user

# Or check port
lsof -i :5432
```

### Check if CubeStore is running

```bash
# Check WebSocket connection
curl http://localhost:3030/

# Or check process
ps aux | grep cubestore

# Check port
lsof -i :3030
```

### Enable Debug Logging

```bash
# CubeSQL internal debugging
export CUBEJS_NATIVE_INTERNAL_DEBUG=true

# Cube.js log level
export CUBEJS_LOG_LEVEL=trace

# CubeStore logs
export CUBESTORE_LOG_LEVEL=trace
```

### Common Issues

1. **CubeStore connection failed**
   ```
   Error: Cube Store was specified as queue/cache driver.
   Please set CUBEJS_CUBESTORE_HOST and CUBEJS_CUBESTORE_PORT
   ```
   **Solution**: Start CubeStore or set to memory driver:
   ```bash
   export CUBEJS_CACHE_AND_QUEUE_DRIVER=memory
   ```

2. **Port already in use**
   ```
   Error: Address already in use (port 5432)
   ```
   **Solution**: Change port or kill existing process:
   ```bash
   export CUBEJS_PG_SQL_PORT=15432
   # Or for ADBC(Arrow Native) port:
   export CUBEJS_ADBC_PORT=18120
   ```

3. **Native module not found**
   ```
   Error: Unable to load @cubejs-backend/native
   ```
   **Solution**: Rebuild native module:
   ```bash
   cd packages/cubejs-backend-native
   yarn run native:build
   ```

## Performance Considerations

### CubeSQL (Embedded)
- ✅ Zero-copy data transfer between Node.js and Rust
- ✅ No network overhead
- ✅ Direct memory access
- ⚠️ Shares memory with Node.js process

### CubeStore (External)
- ✅ Isolated process with dedicated resources
- ✅ Can be scaled independently
- ✅ Persistent storage for pre-aggregations
- ⚠️ WebSocket communication overhead
- ⚠️ Network latency for queries

### Recommendations

**Development:**
```bash
# Use memory driver for simplicity
export CUBEJS_CACHE_AND_QUEUE_DRIVER=memory
```

**Production:**
```bash
# Use CubeStore for persistence and scale
export CUBEJS_CACHE_AND_QUEUE_DRIVER=cubestore
export CUBEJS_CUBESTORE_HOST=cubestore-host
```

**High Performance:**
```bash
# Enable ADBC(Arrow Native) for better performance
export CUBEJS_ADBC_PORT=8120

# Connect using ADBC (Arrow Database Connectivity) instead of PostgreSQL wire
# ~25-66x faster than HTTP API for large result sets
```

## Related Documentation

- [CubeSQL Architecture](../../../rust/cubesql/README.md)
- [CubeStore Architecture](../../../rust/cubestore/README.md)
- [ADBC(Arrow Native) Protocol](./ARROW_IPC_PROTOCOL.md)
- [Deployment Guide](https://cube.dev/docs/deployment)

## References

### Source Code Locations

| Component | Path |
|-----------|------|
| CubeSQL Native Interface | `packages/cubejs-backend-native/js/index.ts` |
| SQL Server Registration | `packages/cubejs-api-gateway/src/sql-server.ts` |
| CubeStore Driver | `packages/cubejs-cubestore-driver/src/CubeStoreDriver.ts` |
| Query Orchestrator | `packages/cubejs-query-orchestrator/src/orchestrator/QueryOrchestrator.ts` |
| CubeSQL Rust Code | `rust/cubesql/` |
| CubeStore Rust Code | `rust/cubestore/` |

---

**Last Updated**: 2024-12-26
**Cube.js Version**: 0.36.x
**Author**: Architecture Documentation Team
