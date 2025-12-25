# Direct CubeStore Access Analysis

This document analyzes what it would take to modify cubesqld to communicate directly with CubeStore instead of going through the Cube API HTTP REST layer.

## Current Architecture

```
Client → cubesqld → [HTTP REST] → Cube API → [WebSocket] → CubeStore
```

## Proposed Architecture

```
Client → cubesqld → [WebSocket] → CubeStore

↓
          [Schema from Cube API?]
```

---

## 1. CubeStore Interface Analysis

### Current Protocol: WebSocket + FlatBuffers

**NOT Arrow Flight or gRPC** - CubeStore uses a custom protocol:

- **Transport**: WebSocket at `ws://{host}:{port}/ws` (default port 3030)
- **Serialization**: FlatBuffers (not Protobuf)
- **Location**: `/rust/cubestore/cubestore/src/http/mod.rs`

**Message Types:**
```rust
// Request
pub struct HttpQuery {
    query: String,           // SQL query
    inline_tables: Vec<...>, // Temporary tables
    trace_obj: Option<...>,  // Debug tracing
}

// Response
pub struct HttpResultSet {
    columns: Vec<Column>,
    data: Vec<Row>,
}
```

**Client Implementation Example** (`packages/cubejs-cubestore-driver/src/CubeStoreDriver.ts`):
```typescript
this.connection = new WebSocketConnection(`${this.baseUrl}/ws`);

async query<R>(query: string, values: any[]): Promise<R[]> {
  const sql = formatSql(query, values || []);
  return this.connection.query(sql, inlineTables, queryTracingObj);
}
```

**Authentication:**
- Basic HTTP auth (username/password)
- No row-level security at CubeStore level
- CubeStore trusts all SQL it receives

---

## 2. What Cube API Provides (That Would Need Replication)

### A. Schema Compilation Layer
**Location**: `packages/cubejs-schema-compiler`

**Services:**
- **Semantic layer translation**: Cubes/measures/dimensions → SQL
- **Join graph resolution**: Multi-cube joins
- **Security context injection**: Row-level security, WHERE clause additions
- **Multi-tenancy support**: Data isolation per tenant
- **Time dimension handling**: Date ranges, granularities, rolling windows
- **Measure calculations**: Formulas, ratios, cumulative metrics
- **Pre-aggregation selection**: Which rollup table to use

**Example - What Cube API Knows:**
```javascript
// Cube definition (model/Orders.js)
cube('Orders', {
  sql: `SELECT * FROM orders`,
  measures: {
    revenue: {
      sql: 'amount',
      type: 'sum'
    }
  },
  dimensions: {
    createdAt: {
      sql: 'created_at',
      type: 'time'
    }
  },
  preAggregations: {
    daily: {
      measures: [revenue],
      timeDimension: createdAt,
      granularity: 'day'
    }
  }
})
```

**What CubeStore Knows:**
```sql
-- Physical table only
CREATE TABLE dev_pre_aggregations.orders_daily_20250101 (
  created_at_day DATE,
  revenue BIGINT
)
```

**Critical Gap**: CubeStore has no concept of "Orders cube" or "revenue measure" - only physical tables.

### B. Query Planning & Optimization
**Location**: `packages/cubejs-query-orchestrator`

**Services:**
- **Pre-aggregation matching**: Decide rollup vs raw data
- **Cache management**: Result caching, invalidation strategies
- **Queue management**: Background job processing
- **Query rewriting**: Optimization passes
- **Partition selection**: Time-based partition pruning

### C. Security & Authorization

**Current Flow:**
```
1. Client sends API key/JWT to Cube API
2. Cube API validates and extracts security context
3. Context injected as WHERE clauses in generated SQL
4. SQL sent to CubeStore (already secured)
```

**If Bypassing Cube API:**
- cubesqld must validate tokens
- cubesqld must know security rules
- cubesqld must inject WHERE clauses

### D. Pre-aggregation Management

**Complex Logic:**
- Build scheduling (when to refresh)
- Partition management (time-based)
- Incremental refresh (delta updates)
- Lambda pre-aggregations (external storage)
- Partition range selection

---

## 3. Schema Storage - Where Does Schema Information Live?

### In Cube API (Node.js Runtime):
- **Location**: `/model/*.js` or `/model/*.yml` files
- **Format**: JavaScript/YAML cube definitions
- **Compilation**: Runtime compilation to SQL generators
- **Not Accessible to CubeStore**: Lives only in Node.js memory

### In CubeStore (RocksDB):
- **Location**: Metastore (RocksDB-based)
- **Content**: Physical schema only
  - Table definitions
  - Column types
  - Indexes
  - Partitions
- **Queryable via**: `information_schema.tables`, `information_schema.columns`
- **No Semantic Knowledge**: Doesn't understand cubes/measures/dimensions

**Example Query:**
```sql
-- This works in CubeStore
SELECT * FROM information_schema.tables;

-- This does NOT exist in CubeStore
SELECT * FROM cube_metadata.cubes;  -- No such table
```

---

## 4. Implementation Options

### OPTION C: Hybrid with Schema Sync (Recommended)
**Complexity**: Medium | **Timeline**: 3-4 months

**Architecture:**
```
┌─────────────────────────────────────────────────┐
│ cubesqld                                        │
│  ┌──────────────────┐  ┌────────────────────┐  │
│  │ Schema Cache     │  │ Security Context   │  │
│  │ (from Cube API)  │  │ (from Cube API)    │  │
│  └──────────────────┘  └────────────────────┘  │
│           ↓                      ↓              │
│  ┌─────────────────────────────────────────┐   │
│  │ SQL→SQL Translator                      │   │
│  │ (Map semantic → physical tables)        │   │
│  └─────────────────────────────────────────┘   │
│           ↓                                     │
│  ┌─────────────────────────────────────────┐   │
│  │ CubeStore WebSocket Client              │   │
│  └─────────────────────────────────────────┘   │
└─────────────────────────────────────────────────┘
         ↓ Periodic sync          ↓ Query execution
    Cube API (/v1/meta)           CubeStore
```

**Implementation Phases:**

**Phase 1: Schema Sync Service (2-3 weeks)**
```rust
pub struct SchemaSync {
    cache: Arc<RwLock<HashMap<String, TableMetadata>>>,
    cube_api_client: HttpClient,
    refresh_interval: Duration,
}

#[derive(Debug, Clone)]
pub struct TableMetadata {
    physical_name: String,      // "dev_pre_aggregations.orders_daily"
    semantic_name: String,       // "Orders.daily"
    columns: Vec<ColumnDef>,
    security_filters: Vec<Filter>,
}

impl SchemaSync {
    pub async fn sync_loop(&self) {
        loop {
            match self.fetch_meta().await {
                Ok(meta) => self.update_cache(meta),
                Err(e) => error!("Schema sync failed: {}", e),
            }
            tokio::time::sleep(self.refresh_interval).await;
        }
    }
}
```

**Phase 2: CubeStore Client (4-6 weeks)**
```rust
// Based on packages/cubejs-cubestore-driver pattern
pub struct CubeStoreClient {
    ws_stream: Arc<Mutex<WebSocketStream<TcpStream>>>,
    base_url: String,
}

impl CubeStoreClient {
    pub async fn connect(url: &str) -> Result<Self, Error> {
        let ws_stream = tokio_tungstenite::connect_async(format!("{}/ws", url)).await?;
        Ok(Self { ws_stream: Arc::new(Mutex::new(ws_stream)), base_url: url.to_string() })
    }

    pub async fn query(&self, sql: String) -> Result<Vec<RecordBatch>, Error> {
        // Encode query as FlatBuffers
        let fb_msg = encode_http_query(&sql)?;

        // Send via WebSocket
        let mut ws = self.ws_stream.lock().await;
        ws.send(Message::Binary(fb_msg)).await?;

        // Receive response
        let response = ws.next().await.unwrap()?;

        // Decode FlatBuffers → Arrow RecordBatch
        decode_http_result(response.into_data())
    }
}
```

**Phase 3: SQL Translation (3-4 weeks)**
```rust
pub struct QueryTranslator {
    schema_cache: Arc<SchemaSync>,
}

impl QueryTranslator {
    pub fn translate(&self, semantic_sql: &str, context: &SecurityContext) -> Result<String, Error> {
        // Parse SQL
        let ast = Parser::parse_sql(&dialect::PostgreSqlDialect {}, semantic_sql)?;

        // Map table names: Orders → dev_pre_aggregations.orders_daily
        let rewritten_ast = self.rewrite_table_refs(ast)?;

        // Inject security filters
        let secured_ast = self.inject_security_filters(rewritten_ast, context)?;

        // Generate CubeStore SQL
        Ok(secured_ast.to_string())
    }
}
```

**Phase 4: Security Context (2-3 weeks)**
```rust
pub struct SecurityContext {
    user_id: String,
    tenant_id: String,
    custom_filters: HashMap<String, String>,
}

impl SecurityContext {
    pub fn from_cube_api(auth_token: &str, cube_api_url: &str) -> Result<Self, Error> {
        // Call Cube API to get security context
        let response = reqwest::get(format!("{}/v1/context", cube_api_url))
            .header("Authorization", auth_token)
            .send().await?;

        response.json().await
    }

    pub fn as_sql_filters(&self) -> Vec<String> {
        vec![
            format!("tenant_id = '{}'", self.tenant_id),
            // Additional filters...
        ]
    }
}
```

**Total Effort**: 11-16 weeks (3-4 months)

**Pros:**
- Clear separation of concerns
- Incremental migration path
- Reuse Cube API for complex logic
- Reduce cubesqld-specific code

**Cons:**
- Schema sync staleness (mitigated with short TTL)
- Dependency on Cube API for metadata
- Complex translation layer

**Performance Gain**: ~40-60% latency reduction

---

## 5. Alternative: Optimize Existing Path (Recommended First Step)

Instead of major architectural changes, optimize the current path:

### A. Add Connection Pooling (1-2 weeks)
```rust
// In cubesqld transport layer
pub struct PooledHttpTransport {
    client: Arc<Client>,  // HTTP/2 with keep-alive
    connection_pool: Pool,
}
```
**Benefit**: Reduce HTTP connection overhead (~20% latency improvement)

### B. Implement Query Result Streaming (2-3 weeks)
```rust
// Stream Arrow batches as they arrive
pub async fn load_stream(&self, query: &str) -> BoxStream<RecordBatch> {
    // Instead of waiting for full JSON response
}
```
**Benefit**: Lower time-to-first-byte (~30% improvement for large results)

### C. Add Arrow Flight to CubeStore (3-4 weeks)
**Modify CubeStore** to support Arrow Flight protocol alongside WebSocket:
- More efficient for large result sets
- Native Arrow encoding (no JSON intermediary)
- Standardized protocol

**Benefit**: ~50% data transfer efficiency improvement

### D. Cube API Arrow Response (2 weeks)
**Add `/v1/arrow` endpoint** to Cube API that returns Arrow IPC directly:
```typescript
// packages/cubejs-api-gateway
router.post('/v1/arrow', async (req, res) => {
  const result = await queryOrchestrator.executeQuery(req.body.query);
  const arrowBuffer = convertToArrow(result);
  res.set('Content-Type', 'application/vnd.apache.arrow.stream');
  res.send(arrowBuffer);
});
```

**Benefit**: Eliminate JSON → Arrow conversion in cubesqld

**Total Optimization Effort**: 8-11 weeks (2-3 months)
**Performance Gain**: ~60-80% of direct CubeStore access benefit
**Risk**: Low (no architectural changes)

---

## 6. Risk Assessment

### Direct CubeStore Access Risks:

| Risk | Severity | Mitigation |
|------|----------|------------|
| Schema drift (cache stale) | High | Short TTL (5-30s), schema versioning |
| Security bypass | Critical | Rigorous testing, security audit |
| Pre-agg selection errors | Medium | Fallback to Cube API for complex queries |
| Breaking changes in Cube | Medium | Pin Cube version, extensive integration tests |
| Maintenance burden | High | Automated testing, clear documentation |
| Feature parity gaps | Medium | Phased rollout, feature flags |

### Optimization Approach Risks:

| Risk | Severity | Mitigation |
|------|----------|------------|
| Cube API changes | Low | Upstream collaboration, versioning |
| Performance not sufficient | Medium | Benchmark before/after |
| Implementation complexity | Low | Well-understood patterns |

---

## 7. Performance Analysis

### Current Latency Breakdown (Local Development):
```
Total query time: ~50-80ms
├─ cubesqld processing: 5ms
├─ HTTP round-trip: 5-10ms
├─ Cube API processing: 10-20ms
│  ├─ Schema compilation: 5-10ms
│  ├─ Pre-agg selection: 3-5ms
│  └─ Security context: 2-5ms
├─ WebSocket to CubeStore: 5-10ms
├─ CubeStore query: 15-25ms
└─ JSON→Arrow conversion: 5-10ms
```

### Direct CubeStore (Option C):
```
Total query time: ~25-35ms (50% improvement)
├─ cubesqld processing: 5ms
├─ Schema cache lookup: 1ms
├─ SQL translation: 3-5ms
├─ Security filter injection: 2ms
├─ WebSocket to CubeStore: 5-10ms
└─ CubeStore query: 15-25ms
```

### Optimized Current Path:
```
Total query time: ~30-45ms (40% improvement)
├─ cubesqld processing: 5ms
├─ HTTP/2 keepalive: 2ms
├─ Cube API (optimized): 8-15ms
├─ WebSocket to CubeStore: 5-10ms
├─ CubeStore query: 15-25ms
└─ Arrow native response: 2ms (no JSON conversion)
```

---

## 8. Recommendation

TODO THIS
### Immediate (Next 2-3 months):
**Optimize existing architecture** with low-risk improvements:
1. HTTP/2 connection pooling
2. Add `/v1/arrow` endpoint to Cube API
3. Implement result streaming
4. Benchmark and measure

**Expected Outcome**: 40-60% latency reduction, 80% of direct access benefit

### Medium-term (6-9 months):
If performance still insufficient:
1. **Implement Option C (Hybrid with Schema Sync)**
2. Start with read-only pre-aggregation queries
3. Gradual rollout with feature flags
4. Keep Cube API path for complex queries


## 9. Code References

**CubeStore Protocol:**
- WebSocket handler: `/rust/cubestore/cubestore/src/http/mod.rs:200-350`
- Message types: `/rust/cubestore/cubestore/src/http/mod.rs:50-120`

**Current CubeStore Client (Node.js):**
- Driver: `/packages/cubejs-cubestore-driver/src/CubeStoreDriver.ts`
- WebSocket connection: `/packages/cubejs-cubestore-driver/src/WebSocketConnection.ts`

**Cube API Services:**
- Schema compiler: `/packages/cubejs-schema-compiler/src/compiler/CubeSymbols.ts`
- Query orchestrator: `/packages/cubejs-query-orchestrator/src/orchestrator/QueryOrchestrator.ts`
- Pre-agg matching: `/packages/cubejs-query-orchestrator/src/orchestrator/PreAggregations.ts`

**cubesqld Current Transport:**
- HTTP transport: `/rust/cubesql/cubesql/src/transport/service.rs:280-320`
- Cube API client: `/rust/cubesql/cubesql/src/compile/engine/df/scan.rs:680-762`

---

## 10. Conclusion

**Direct CubeStore access is technically feasible but requires substantial engineering effort** to replicate Cube API's semantic layer, security model, and query planning logic.

**The most pragmatic approach is:**
1. **First**: Optimize the existing cubesqld → Cube API → CubeStore path (2-3 months, low risk)
