# Development

## Build and run

### Prerequisites:

- `rustup`
- `cargo install cargo-insta`

```bash
cd rust/cubesql
CUBESQL_CUBE_URL=$URL/cubejs-api \
CUBESQL_CUBE_TOKEN=$TOKEN \
CUBESQL_LOG_LEVEL=debug \
CUBESQL_BIND_ADDR=0.0.0.0:4444 \
cargo run
```

In a separate terminal, run:

```bash
mysql -u root -h 127.0.0.1 --ssl-mode=disabled -u root --password=test --port 4444
```

# Architecture

## Connections management

```mermaid
classDiagram
    ServerConfiguration <|-- ServerManager
    Transport <|-- ServerManager
    Auth <|-- ServerManager
    class ServerManager {
    }

    ServerManager <|-- SessionManager
    class SessionManager{
        +HashMap<u32, Session> sessions
        +create_session() Session
        +drop_session() void
        +process_list() Vec<SessionProcessList>
    }

    Session <|-- MysqlConnection
    Session <|-- PostgresConnection

    class Session {
        +SessionState state
    }

    class SessionProperties{
        +String readonly user
        +String readonly database
    }

    SessionProperties <|-- SessionState
    AuthContext <|-- SessionState
    class SessionState{
        +U32 connection_id
        +String host
    }

    SessionState <|-- Session
    SessionProperties <|-- Session
    SessionManager <|-- Session
    class Session{
    }
```

## Query Execution

```mermaid
classDiagram
    ServerConfiguration <|-- ServerManager
    Transport <|-- ServerManager
    Auth <|-- ServerManager
    class ServerManager {
    }

    ServerManager <|-- SessionManager
    class SessionManager{
    }

    class MetaContext{
    }

    SessionState <|-- QueryPlanner
    MetaContext <|-- QueryPlanner
    SessionManager <|-- QueryPlanner
    class QueryPlanner{
    }

    CubeQueryPlanner <|-- ExecutionContext
    class ExecutionContext {
    }

    class CubeQueryPlanner {
    }
```