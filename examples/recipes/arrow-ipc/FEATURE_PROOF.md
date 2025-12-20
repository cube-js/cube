<
> Please help me to understand how `cubesqld` part we are working on in this branch used by all other cube
  parts. Who when starts stops in what development topology: one cube js in local docker versus full setup
  od cube api, api workers, cubestore and cubestore workers. the goal is to understand how data passed over
  before served as ARROW IPC by cubesqld.

● I'll help you understand how cubesqld integrates into the Cube architecture and data flow. Let me explore the codebase to map out the components and their interactions.

● Explore(Explore cubesqld architecture integration)
  ⎿  Done (45 tool uses · 85.8k tokens · 3m 19s)

● Based on my exploration of the codebase, I can now explain how cubesqld integrates into the Cube architecture. Here's the complete picture:

  Architecture Overview

  cubesqld is a SQL-to-REST adapter proxy, not a database engine. It translates PostgreSQL and Arrow Native wire protocols into Cube REST API calls.

  Data Flow: Query Execution Pipeline

  ┌─────────────────────────────────────────────────────────────────┐
  │ 1. Client Connection                                            │
  │    psql/Python/R → Arrow Native Protocol (port 4445)            │
  └────────────────────┬────────────────────────────────────────────┘
                       ↓
  ┌─────────────────────────────────────────────────────────────────┐
  │ 2. cubesqld (Rust process)                                      │
  │    • ArrowNativeServer receives SQL query                       │
  │    • Parse SQL → AST (sqlparser-rs)                             │
  │    • Optimize with egg-based rewrite engine                     │
  │    • Build DataFusion physical plan                             │
  │    • CubeScanExecutionPlan created                              │
  └────────────────────┬────────────────────────────────────────────┘
                       ↓ HTTP REST API
  ┌─────────────────────────────────────────────────────────────────┐
  │ 3. Cube API (Node.js process)                                   │
  │    • Receives load_v1() REST API call with Cube query           │
  │    • Query Orchestrator processes request                       │
  │    • Schema Compiler validates against data models              │
  └────────────────────┬────────────────────────────────────────────┘
                       ↓
  ┌─────────────────────────────────────────────────────────────────┐
  │ 4. Data Execution                                               │
  │    • Database driver executes SQL against source DB             │
  │    • Results cached in CubeStore (if configured)                │
  │    • JSON response returned to cubesqld                         │
  └────────────────────┬────────────────────────────────────────────┘
                       ↓
  ┌─────────────────────────────────────────────────────────────────┐
  │ 5. Arrow IPC Streaming (cubesqld)                               │
  │    • Convert JSON → Arrow RecordBatch                           │
  │    • StreamWriter::stream_query_results()                       │
  │      - write_schema() [Arrow IPC schema]                        │
  │      - stream_batches() [Arrow IPC data batches]                │
  │      - write_complete() [completion message]                    │
  └────────────────────┬────────────────────────────────────────────┘
                       ↓
  ┌─────────────────────────────────────────────────────────────────┐
  │ 6. Client Receives Arrow IPC Stream                             │
  └─────────────────────────────────────────────────────────────────┘

  Critical Insight: cubesqld never accesses CubeStore directly. It only communicates with Cube API via HTTP REST.

  Deployment Topologies

  1. Your Current Setup (Local Development - Arrow IPC Recipe)

  ┌──────────────────────┐
  │  PostgreSQL Docker   │  Port 7432
  │  (Source Database)   │
  └──────────┬───────────┘
             ↓ SQL queries
  ┌──────────────────────┐
  │  Cube API Process    │  Port 4008
  │  (Node.js)           │  • Built-in SQL APIs DISABLED
  │  • REST API          │  • cubejs-server-core
  │  • Query Orchestrator│  • Schema compiler
  └──────────┬───────────┘
             ↑ HTTP REST
             │ CUBESQL_CUBE_URL=http://localhost:4008/cubejs-api
  ┌──────────────────────┐
  │  cubesqld Process    │  Ports 4444 (pg), 4445 (arrow)
  │  (Rust)              │  • PostgreSQL wire protocol
  │  • SQL → Cube query  │  • Arrow Native protocol
  │  • Arrow IPC output  │  • Query compilation
  └──────────┬───────────┘
             ↑ psql/Arrow clients
  ┌──────────────────────┐
  │  Your Python/R/JS    │
  │  Arrow IPC Clients   │
  └──────────────────────┘

  Startup sequence (examples/recipes/arrow-ipc/):
  1. start-cube-api.sh - Starts Cube API with SQL ports disabled
  2. start-cubesqld.sh - Starts cubesqld separately as standalone proxy
  3. Clients connect to cubesqld ports (4444 or 4445)

  Configuration:
  # Cube API - SQL protocols disabled
  unset CUBEJS_PG_SQL_PORT  # Let cubesqld handle this
  unset CUBEJS_ARROW_PORT   # Let cubesqld handle this

  # cubesqld - Provides SQL protocols
  CUBESQL_PG_PORT=4444      # PostgreSQL wire protocol
  CUBEJS_ARROW_PORT=4445    # Arrow Native protocol
  CUBESQL_CUBE_URL=http://localhost:4008/cubejs-api
  CUBESQL_CUBE_TOKEN=${CUBE_TOKEN}

  2. Docker Compose Setup

  services:
    postgres:
      image: postgres:14
      ports: ["7432:5432"]

    cube-api:
      environment:
        - CUBEJS_DB_TYPE=postgres
        - CUBEJS_DB_HOST=postgres
        # SQL protocols disabled
      ports: ["4000:4000"]

    cubesqld:
      environment:
        - CUBESQL_CUBE_URL=http://cube-api:4000/cubejs-api/v1
        - CUBESQL_PG_PORT=4444
        - CUBEJS_ARROW_PORT=4445
      ports: ["4444:4444", "4445:4445"]

  3. Production Deployment (Standard Cube - NO cubesqld)

  Important: cubesqld is NOT typically used in production. Standard production architecture:

  Load Balancer
       ↓
  ┌────────────────────────────────────┐
  │  Cube API Cluster (scaled)         │
  │  • Built-in SQL API (optional)     │
  │  • REST/GraphQL APIs               │
  └───────────────┬────────────────────┘
                  ↓ internal network
  ┌────────────────────────────────────┐
  │  CubeStore Cluster                 │
  │  • Router nodes                    │
  │  • Worker nodes                    │
  │  • Pre-aggregation storage         │
  └───────────────┬────────────────────┘
                  ↓
  ┌────────────────────────────────────┐
  │  Source Databases                  │
  └────────────────────────────────────┘

  Why cubesqld isn't in production:
  - Cube API has built-in PostgreSQL/Arrow APIs
  - cubesqld adds extra HTTP hop (latency)
  - Primarily a development/testing tool for SQL compatibility

  Key Source Files

  If you want to trace the code:

  - Startup: rust/cubesql/cubesql/src/bin/cubesqld.rs
  - Arrow Native Server: rust/cubesql/cubesql/src/sql/arrow_native/server.rs:246-298
  - Query Execution: rust/cubesql/cubesql/src/compile/engine/df/scan.rs:680-762
  - Arrow IPC Streaming: rust/cubesql/cubesql/src/sql/arrow_native/stream_writer.rs
  - HTTP Transport: rust/cubesql/cubesql/src/transport/service.rs:280-320

  Component Start/Stop Lifecycle

  Start (cubesqld.rs:49-68):
  1. Read env vars (CUBESQL_CUBE_URL, ports, etc.)
  2. Create service container (Config::default().configure())
  3. Spawn PostgreSQL server processing loop (if CUBESQL_PG_PORT set)
  4. Spawn Arrow Native server processing loop (if CUBEJS_ARROW_PORT set)
  5. Each server runs independently, listening on TCP

  Stop (graceful shutdown):
  - Ctrl+C triggers shutdown handler
  - Smart mode: Wait for in-flight queries to complete
  - Fast mode: Close connections immediately
  - 3x Ctrl+C forces immediate exit

  Summary

  In your Arrow IPC recipe:
  1. Source DB (PostgreSQL) holds raw data
  2. Cube API queries the DB, applies semantic layer, returns JSON
  3. cubesqld acts as a protocol translator: SQL/Arrow wire protocol → Cube REST API
  4. Arrow IPC is streamed back to clients with zero-copy efficiency

  The benefit of your architecture: clients get native Arrow IPC (columnar, zero-copy) while Cube API handles all the semantic layer logic.
