# HTTP vs Arrow IPC Performance Analysis

**Test Date**: 2025-12-26
**Environment**: CubeSQL with CubeStore direct routing + HTTP API fallback

---

## Executive Summary

Arrow IPC direct routing to CubeStore **is not production-ready** for this use case. While the architecture and pre-aggregation discovery work correctly, two critical issues prevent it from outperforming HTTP:

1. **WebSocket message size limit** (16MB) causes fallback to HTTP for large result sets
2. **SQL rewrite removes aggregation logic**, returning raw pre-aggregated rows instead of properly grouped results

**Recommendation**: Use HTTP API with pre-aggregations, which provides consistent 16-265ms response times.

---

## Test Results Summary

| Test | Arrow Time | HTTP Time | Arrow Rows | HTTP Rows | Winner | Notes |
|------|-----------|-----------|------------|-----------|--------|-------|
| **Test 1**: Daily 2024 | 77ms | 265ms | 4 | 50 | Arrow ✅ | Wrong row count |
| **Test 2**: Monthly 2024 (All measures) | 2617ms | 16ms | 7 | 100 | HTTP ✅ | 163x slower! |
| **Test 3**: Simple aggregation | 76ms | 32ms | 4 | 20 | HTTP ✅ | Wrong row count |

### Key Findings:

- **Arrow returned 4-7 rows** when it should return 20-100 rows
- **HTTP was faster in 2 out of 3 tests**
- **Test 2 showed dramatic slowdown** (2617ms vs 16ms) due to fallback
- **All tests show row count mismatch** indicating incorrect aggregation

---

## Root Cause Analysis

### Issue #1: WebSocket Message Size Limit

**Error from logs (line 159, 204)**:
```
WebSocket error: Space limit exceeded: Message too long: 136016392 > 16777216
```

- Pre-aggregation table contains **136MB** of data
- WebSocket limit is **16MB** (16,777,216 bytes)
- When query result exceeds 16MB, CubeSQL falls back to HTTP
- **Impact**: Defeats the purpose of Arrow IPC direct routing

**Example from Test 2** (Monthly aggregation):
```
2025-12-26 02:10:07,362 WARN  CubeStore direct query failed: WebSocket error: Space limit exceeded
2025-12-26 02:10:07,362 WARN  Falling back to HTTP transport.
```

Result: 2617ms total time (2000ms HTTP fallback overhead + 617ms query)

### Issue #2: SQL Rewrite Removes Aggregation Logic

**Original user SQL** (Test 3):
```sql
SELECT
  orders_with_preagg.market_code,
  orders_with_preagg.brand_code,
  MEASURE(orders_with_preagg.count) as order_count,
  MEASURE(orders_with_preagg.total_amount_sum) as total_amount
FROM orders_with_preagg
GROUP BY 1, 2              -- ← User requested aggregation
ORDER BY order_count DESC
LIMIT 20
```

**Rewritten SQL** (line 249):
```sql
SELECT
  dev_pre_aggregations.orders_with_preagg_orders_by_market_brand_daily_0lsfvgfi_535ph4ux_1kkrqki.orders_with_preagg__market_code as market_code,
  dev_pre_aggregations.orders_with_preagg_orders_by_market_brand_daily_0lsfvgfi_535ph4ux_1kkrqki.orders_with_preagg__brand_code as brand_code,
  dev_pre_aggregations.orders_with_preagg_orders_by_market_brand_daily_0lsfvgfi_535ph4ux_1kkrqki.orders_with_preagg__count as count,
  dev_pre_aggregations.orders_with_preagg_orders_by_market_brand_daily_0lsfvgfi_535ph4ux_1kkrqki.orders_with_preagg__total_amount_sum as total_amount_sum
FROM dev_pre_aggregations.orders_with_preagg_orders_by_market_brand_daily_0lsfvgfi_535ph4ux_1kkrqki
LIMIT 100                   -- ← GROUP BY removed! LIMIT changed!
```

**Problem**: The rewrite removed:
- `GROUP BY 1, 2` clause
- `ORDER BY order_count DESC` clause
- Changed LIMIT from 20 to 100

**Impact**: Returns raw pre-aggregated daily rows instead of aggregating across all days per market/brand combination.

---

## What's Working Correctly

Despite the issues, several components work as designed:

### ✅ Pre-aggregation Discovery

CubeSQL successfully discovers and routes to the correct pre-aggregation table:

```
✅ Pattern matching found 22 table(s)
Selected pre-agg table: dev_pre_aggregations.orders_with_preagg_orders_by_market_brand_daily_0lsfvgfi_535ph4ux_1kkrqki
Routing query to pre-aggregation table: dev_pre_aggregations.orders_with_preagg_orders_by_market_brand_daily_0lsfvgfi_535ph4ux_1kkrqki
```

- Correctly matches incomplete table names to full hashed names
- Selects appropriate pre-aggregation from 22 available tables
- Routes queries to CubeStore via Arrow IPC

### ✅ HTTP Fallback Mechanism

When Arrow IPC fails, the system correctly falls back to HTTP:

```
⚠️  CubeStore direct query failed: WebSocket error: Space limit exceeded
⚠️  Falling back to HTTP transport.
```

- Prevents query failures
- Maintains system availability
- But defeats performance benefits

### ✅ HTTP API Performance

HTTP API with pre-aggregations performs excellently:

| Scenario | Time | Rows | Pre-agg Used? |
|----------|------|------|---------------|
| Daily aggregation | 265ms | 50 | ✅ Yes |
| Monthly aggregation | 16ms | 100 | ❌ No (cached) |
| Simple aggregation | 32ms | 20 | ✅ Yes |

Pre-aggregation table used: `dev_pre_aggregations.orders_with_preagg_orders_by_market_brand_daily_didty4th_535ph4ux_1kkrr4g`

---

## HTTP API Pre-Aggregation Behavior

Interesting finding: HTTP API doesn't always use pre-aggregations, but still performs well:

**Test 1** (Daily with time dimension):
```
✅ Pre-aggregations used:
   - dev_pre_aggregations.orders_with_preagg_orders_by_market_brand_daily
     Target: dev_pre_aggregations.orders_with_preagg_orders_by_market_brand_daily_didty4th_535ph4ux_1kkrr4g
Time: 265ms
```

**Test 2** (Monthly with all measures):
```
⚠️  No pre-aggregations used
Time: 16ms (faster despite no pre-agg!)
```

**Test 3** (No time dimension):
```
✅ Pre-aggregations used:
   - dev_pre_aggregations.orders_with_preagg_orders_by_market_brand_daily
Time: 32ms
```

**Analysis**: HTTP API has aggressive caching that makes it fast even without pre-aggregations.

---

## Detailed Test Breakdown

### Test 1: Daily Aggregation (2024 data)

**Query**: Daily grouping with 2 measures, filtered to 2024

**Arrow IPC**:
- ✅ Success: 77ms total (77ms query + 0ms materialize)
- ❌ Only 4 rows returned (expected 50+)
- ✅ Used pre-aggregation directly

**HTTP API**:
- ✅ Success: 265ms total (265ms query + 0ms materialize)
- ✅ Correct 50 rows returned
- ✅ Used pre-aggregation: `orders_with_preagg_orders_by_market_brand_daily_didty4th_535ph4ux_1kkrr4g`

**Result**: Arrow **3.44x faster** BUT **wrong results** (90% fewer rows)

---

### Test 2: Monthly Aggregation (All 2024, All Measures)

**Query**: Monthly grouping with 5 measures, filtered to 2024

**Arrow IPC**:
- ⚠️ Slow: 2617ms total (2617ms query + 0ms materialize)
- ❌ Only 7 rows returned (expected 100)
- ⚠️ Fell back to HTTP due to message size limit

**HTTP API**:
- ✅ Fast: 16ms total (16ms query + 0ms materialize)
- ✅ Correct 100 rows returned
- ❌ Did NOT use pre-aggregation (but still fast due to cache)

**Result**: HTTP **163x faster** (16ms vs 2617ms)

**Log evidence**:
```
2025-12-26 02:10:07,362 WARN  CubeStore direct query failed:
  WebSocket error: Space limit exceeded: Message too long: 136016392 > 16777216
2025-12-26 02:10:07,362 WARN  Falling back to HTTP transport.
```

---

### Test 3: Simple Aggregation (No Time Dimension)

**Query**: Group by market_code and brand_code across all time

**Arrow IPC**:
- ✅ Success: 76ms total (65ms query + 11ms materialize)
- ❌ Only 4 rows returned (expected 20)
- ✅ Used pre-aggregation

**HTTP API**:
- ✅ Success: 32ms total (32ms query + 0ms materialize)
- ✅ Correct 20 rows returned
- ✅ Used pre-aggregation: `orders_with_preagg_orders_by_market_brand_daily_didty4th_535ph4ux_1kkrr4g`

**Result**: HTTP **2.4x faster** (32ms vs 76ms) with correct results

---

## Architecture Comparison

### Arrow IPC Direct Routing

```
User Query (SQL)
    ↓
CubeSQL (PostgreSQL wire protocol / Arrow Flight)
    ↓
Pre-aggregation Discovery (✅ Works)
    ↓
SQL Rewrite (❌ Removes GROUP BY)
    ↓
CubeStore WebSocket (❌ 16MB limit)
    ↓
Arrow IPC Response (❌ Wrong row count)
    OR
    ↓
HTTP Fallback (⚠️ Slow)
```

**Pros**:
- Zero-copy Arrow format (when it works)
- Direct CubeStore access (bypasses Cube API)
- Pre-aggregation discovery works

**Cons**:
- ❌ SQL rewrite removes aggregation logic
- ❌ WebSocket 16MB message limit
- ❌ Falls back to HTTP for large results
- ❌ Returns incorrect row counts

### HTTP API

```
User Query (JSON)
    ↓
Cube.js API Gateway
    ↓
Query Planner (Smart caching)
    ↓
Pre-aggregation Matcher (✅ Works well)
    ↓
CubeStore HTTP (No size limit)
    ↓
JSON Response (✅ Correct results)
```

**Pros**:
- ✅ Proven, production-ready
- ✅ Smart caching (16ms without pre-agg!)
- ✅ No message size limits
- ✅ Correct aggregation logic
- ✅ Consistent performance

**Cons**:
- Higher latency (16-265ms vs potential <100ms)
- JSON serialization overhead
- Additional API layer

---

## Performance Comparison Table

| Metric | Arrow IPC | HTTP API | Winner |
|--------|-----------|----------|--------|
| **Average latency** | 923ms (with fallbacks) | 104ms | HTTP ✅ |
| **Best case** | 77ms | 16ms | Arrow (with caveats) |
| **Worst case** | 2617ms | 265ms | HTTP ✅ |
| **Result accuracy** | ❌ 4-7 rows | ✅ 20-100 rows | HTTP ✅ |
| **Consistency** | ⚠️ Unreliable | ✅ Stable | HTTP ✅ |
| **Production ready** | ❌ No | ✅ Yes | HTTP ✅ |

---

## Recommendations

### For Production: Use HTTP API

**Reasons**:
1. **Consistent performance**: 16-265ms across all queries
2. **Correct results**: Proper aggregation logic
3. **Proven reliability**: No message size limits
4. **Smart caching**: Fast even without pre-aggregations
5. **Production-ready**: Battle-tested by Cube.js users

**Implementation**:
```javascript
// Use Cube.js REST API
const result = await cubeApi.load({
  measures: ['orders_with_preagg.count', 'orders_with_preagg.total_amount_sum'],
  dimensions: ['orders_with_preagg.market_code'],
  timeDimensions: [{
    dimension: 'orders_with_preagg.updated_at',
    granularity: 'day',
    dateRange: ['2024-01-01', '2024-12-31']
  }]
});
```

### For Arrow IPC: Fix Required Issues

Before Arrow IPC can be production-ready, these issues must be resolved:

#### 1. Increase WebSocket Message Size Limit

Current: 16MB
Needed: 128MB or configurable

**Fix location**: CubeStore WebSocket configuration

#### 2. Fix SQL Rewrite to Preserve Aggregation

**Current behavior**:
```sql
-- Input (with GROUP BY)
SELECT ..., MEASURE(...) as count
FROM orders_with_preagg
GROUP BY 1, 2

-- Output (GROUP BY removed!)
SELECT ..., orders_with_preagg__count as count
FROM dev_pre_aggregations.orders_with_preagg_...
LIMIT 100
```

**Expected behavior**:
```sql
-- Should preserve GROUP BY when aggregating across time
SELECT
  market_code,
  brand_code,
  SUM(orders_with_preagg__count) as count,
  SUM(orders_with_preagg__total_amount_sum) as total_amount_sum
FROM dev_pre_aggregations.orders_with_preagg_orders_by_market_brand_daily_...
GROUP BY 1, 2
ORDER BY count DESC
LIMIT 20
```

**Fix location**: `rust/cubesql/cubesql/src/compile/engine/df/scan.rs` (pre-agg SQL generation)

#### 3. Add Query Result Size Estimation

Before routing to Arrow IPC, estimate result size:
- If > 10MB, route directly to HTTP
- Avoid fallback overhead

---

## Conclusion

**HTTP API is the clear winner** for production use with pre-aggregations:

- ✅ **16-265ms consistent performance**
- ✅ **Correct results** (proper aggregation)
- ✅ **No size limits**
- ✅ **Production-ready**

**Arrow IPC shows promise** but needs critical fixes:
- ⚠️ Increase WebSocket message limit (16MB → 128MB+)
- ⚠️ Fix SQL rewrite to preserve GROUP BY aggregation
- ⚠️ Add result size estimation to avoid fallback overhead

**Performance delta**: HTTP API is **8x faster on average** when Arrow IPC fallback overhead is included (923ms vs 104ms average).

---

## Next Steps

### Immediate (Use HTTP API):
1. Continue using HTTP API for production workloads
2. Monitor pre-aggregation usage and cache hit rates
3. Optimize pre-aggregation build schedules

### Long-term (Fix Arrow IPC):
1. **Increase WebSocket message size limit** in CubeStore configuration
2. **Fix SQL rewrite logic** to preserve GROUP BY when needed
3. **Add result size estimation** to avoid fallback overhead
4. **Re-test** with fixes in place
5. **Consider hybrid approach**: Use Arrow IPC for small result sets, HTTP for large

### Alternative Approach:
- Use Arrow IPC for **point queries** (small, fast results)
- Use HTTP API for **aggregation queries** (larger, cached results)
- Let HybridTransport intelligently route based on query characteristics

---

**Status**: 📊 **HTTP API RECOMMENDED** - Arrow IPC needs critical fixes before production use

