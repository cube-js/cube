# Complete Value Chain: power_of_3 → ADBC → cubesqld → CubeStore

**Date**: December 25, 2025
**Goal**: Transparent pre-aggregation routing for power_of_3 queries

---

## Current Architecture

```
power_of_3 (Elixir)
  ↓ generates Cube SQL with MEASURE() syntax
  ↓ Example: "SELECT customer.brand, MEASURE(customer.count) FROM customer GROUP BY 1"
  ↓
ADBC (Arrow Native protocol)
  ↓ sends to cubesqld:4445
  ↓
cubesqld
  ↓ Currently: compiles to Cube REST API calls → HttpTransport
  ↓ Goal: detect pre-agg → compile to SQL → CubeStoreTransport
  ↓
Cube API (HTTP/JSON) OR CubeStore (Arrow/FlatBuffers)
```

---

## What power_of_3 Does

### 1. QueryBuilder Generates Cube SQL
From `/home/io/projects/learn_erl/power-of-three/lib/power_of_three/query_builder.ex`:

```elixir
QueryBuilder.build(
  cube: "customer",
  columns: [
    %DimensionRef{name: :brand, ...},
    %MeasureRef{name: :count, ...}
  ],
  where: "brand_code = 'NIKE'",
  limit: 10
)
# => "SELECT customer.brand, MEASURE(customer.count) FROM customer
#     WHERE brand_code = 'NIKE' GROUP BY 1 LIMIT 10"
```

### 2. CubeConnection Executes via ADBC
From `/home/io/projects/learn_erl/power-of-three/lib/power_of_three/cube_connection.ex`:

```elixir
{:ok, conn} = CubeConnection.connect(
  host: "localhost",
  port: 4445,  # cubesqld Arrow Native port
  token: "test"
)

{:ok, result} = CubeConnection.query(conn, cube_sql)
# Internally: Adbc.Connection.query(conn, cube_sql)
```

### 3. Result Converted to DataFrame
power_of_3 gets results as Arrow RecordBatches and converts to Explorer DataFrames

---

## The Problem

When cubesqld receives Cube SQL queries (with MEASURE syntax):

**Current Behavior**:
1. cubesqld parses the MEASURE query
2. Compiles it to Cube REST API format
3. Sends to HttpTransport → Cube API → JSON overhead

**Desired Behavior**:
1. cubesqld parses the MEASURE query
2. **Detects if pre-aggregation available**
3. If yes: compiles to SQL targeting pre-agg table → CubeStoreTransport → Arrow/FlatBuffers (fast!)
4. If no: falls back to HttpTransport (compatible)

---

## The Solution

### Where the Magic Needs to Happen

The routing decision must occur in **cubesql's query compilation pipeline**, not at the transport layer.

Location: `/home/io/projects/learn_erl/cube/rust/cubesql/cubesql/src/compile/`

```
User Query (MEASURE syntax)
  ↓
SQL Parser
  ↓
Query Rewriter (egg-based optimization)
  ↓
*** HERE: Check for pre-aggregation availability ***
  ↓
Compilation:
  - If pre-agg available: generate SQL → CubeStoreTransport
  - If not: generate REST call → HttpTransport
```

### Required Changes

1. **Pre-Aggregation Detection** (in compilation phase)
   - Query metadata to find available pre-aggregations
   - Match query requirements to pre-agg capabilities
   - Decide routing strategy

2. **SQL Generation for Pre-Aggregations**
   - Compile MEASURE query to standard SQL
   - Target pre-aggregation table name
   - Map cube fields to pre-agg field names (e.g., `cube__field`)

3. **Transport Selection**
   - Pass generated SQL to transport layer
   - CubeStoreTransport handles queries WITH SQL
   - HttpTransport handles queries WITHOUT SQL (fallback)

---

## Why HybridTransport Alone Isn't Enough

Initially, I tried creating a HybridTransport that routes based on whether SQL is provided. **This is necessary but not sufficient**:

**HybridTransport handles**: "Given SQL or not, which transport to use?"
**But we still need**: "Should we generate SQL for this MEASURE query?"

The real intelligence must be in the **compilation phase**, which:
- Understands the semantic query
- Knows about pre-aggregations
- Can generate optimized SQL

Then HybridTransport simply routes based on that decision.

---

## Implementation Plan

### Phase 1: Complete HybridTransport (Routing Layer) ✅
- [x] Created HybridTransport skeleton
- [ ] Implement all TransportService trait methods
- [ ] Build and test routing logic
- [ ] Deploy to cubesqld

### Phase 2: Pre-Aggregation Detection (Compilation Layer)
- [ ] Explore cubesql compilation pipeline
- [ ] Find where queries are compiled to REST API
- [ ] Add pre-aggregation metadata lookup
- [ ] Implement pre-agg matching logic

### Phase 3: SQL Generation for Pre-Aggregations
- [ ] Generate SQL targeting pre-agg tables
- [ ] Handle field name mapping (cube.field → cube__field)
- [ ] Pass SQL to transport layer

### Phase 4: End-to-End Testing
- [ ] Test with power_of_3 queries
- [ ] Verify transparent routing
- [ ] Benchmark performance improvements
- [ ] Document results

---

## Expected Outcome

**For power_of_3 users: Zero changes required!**

```elixir
# Same query as before
{:ok, df} = PowerOfThree.DataFrame.new(
  cube: Customer,
  select: [:brand, :count],
  where: "brand_code = 'NIKE'",
  limit: 10
)

# But now:
# - If pre-aggregation exists: ~5x faster (Arrow/FlatBuffers, pre-agg table)
# - If not: same speed as before (HTTP/JSON, source database)
# - Completely transparent!
```

---

## Current Status

✅ **Completed**:
- CubeStoreTransport implementation
- Integration into cubesqld config
- Di power_of_3 value chain understanding

🔄 **In Progress**:
- HybridTransport implementation
- Transport routing logic

⏳ **Next**:
- Compilation pipeline exploration
- Pre-aggregation detection
- SQL generation for MEASURE queries

---

## Files to Explore Next

### cubesql Compilation Pipeline
1. `/home/io/projects/learn_erl/cube/rust/cubesql/cubesql/src/compile/parser/` - SQL parsing
2. `/home/io/projects/learn_erl/cube/rust/cubesql/cubesql/src/compile/rewrite/` - Query rewriting
3. `/home/io/projects/learn_erl/cube/rust/cubesql/cubesql/src/compile/engine/` - Query execution
4. `/home/io/projects/learn_erl/cube/rust/cubesql/cubesql/src/sql/` - SQL protocol handling

### Key Questions
1. Where does cubesql compile MEASURE syntax to REST API calls?
2. Where does it fetch metadata about cubes and pre-aggregations?
3. Can we inject pre-aggregation selection logic there?
4. How to generate SQL for pre-agg tables?

---

**Next Step**: Explore cubesql compilation pipeline to find where MEASURE queries are processed and where we can inject pre-aggregation routing logic.
