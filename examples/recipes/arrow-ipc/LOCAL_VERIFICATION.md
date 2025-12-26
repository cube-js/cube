# Local PR Verification Guide

This guide explains how to verify the **CubeSQL Arrow Native Server** PR locally, including the optional query cache feature.

## Complete Verification Checklist

### ✅ Step 1: Build and Test Rust Code

```bash
cd rust/cubesql

# Run formatting check
cargo fmt --all --check

# Run clippy with strict warnings
cargo clippy --all -- -D warnings

# Build release binary
cargo build --release

# Run unit tests
cargo test arrow_native::cache
```

**Expected results**:
- ✅ All files formatted correctly
- ✅ Zero clippy warnings
- ✅ Clean release build
- ✅ All cache tests passing

### ✅ Step 2: Set Up Test Environment

```bash
cd ../../examples/recipes/arrow-ipc

# Start PostgreSQL
docker-compose up -d postgres

# Wait for database to be ready
sleep 5

# Load sample data
./setup_test_data.sh
```

**Expected output**:
```
✓ Database ready with 3000 orders

Next steps:
  1. Start Cube API: ./start-cube-api.sh
  2. Start CubeSQL: ./start-cubesqld.sh
  3. Run Python tests: python test_arrow_native_performance.py
```

### ✅ Step 3: Verify Arrow Native Server

**Start Cube API** (Terminal 1):
```bash
./start-cube-api.sh
```

**Start CubeSQL Arrow Native Server** (Terminal 2):
```bash
./start-cubesqld.sh
```

**Look for in logs**:
```
🔗 Cube SQL (pg) is listening on 0.0.0.0:4444
🔗 Cube SQL (arrow) is listening on 0.0.0.0:4445
Query result cache: ENABLED (max_entries=1000, ttl=3600s)
```

**Verify server is running**:
```bash
lsof -i:4444  # PostgreSQL protocol
lsof -i:4445  # Arrow IPC native
grep "Query result cache:" cubesqld.log  # Optional cache
```

### ✅ Step 4: Run Python Performance Tests

```bash
# Install Python dependencies
python3 -m venv .venv
source .venv/bin/activate
pip install psycopg2-binary requests

# Run tests
python test_arrow_native_performance.py
```

**Expected results**:
```
CUBESQL ARROW NATIVE SERVER PERFORMANCE TEST SUITE
==================================================

TEST: Query Cache (Optional Feature)
-------------------------------------
First query:  1200-2500ms  (cache miss)
Second query:  200-500ms   (cache hit)
Speedup:      3-10x faster ✓

TEST: CubeSQL vs REST HTTP API
-------------------------------
Small queries:   10-20x faster ✓
Medium queries:   8-15x faster ✓
Large queries:    3-8x faster ✓

Average Speedup: 8-15x

✓ All tests passed!
```

### ✅ Step 5: Manual Cache Verification

**Test cache behavior directly**:

```bash
# Connect to CubeSQL
psql -h 127.0.0.1 -p 4444 -U username

# Enable query timing
\timing on

# Run a query (cache MISS)
SELECT market_code, COUNT(*) FROM orders_with_preagg 
WHERE updated_at >= '2024-01-01' LIMIT 100;
-- Time: 800-1500 ms

# Run exact same query (cache HIT)
SELECT market_code, COUNT(*) FROM orders_with_preagg 
WHERE updated_at >= '2024-01-01' LIMIT 100;
-- Time: 100-300 ms (much faster!)

# Run similar query with different whitespace (cache HIT)
SELECT   market_code,   COUNT(*)   FROM   orders_with_preagg 
WHERE   updated_at   >=   '2024-01-01'   LIMIT   100;
-- Time: 100-300 ms (still cached!)
```

## Detailed Verification Steps

### Verify Cache Hits in Logs

**Enable debug logging**:
```bash
export CUBESQL_LOG_LEVEL=debug
./start-cubesqld.sh
```

**Run a query, check logs**:
```bash
tail -f cubesqld.log | grep -i cache
```

**Expected log output**:
```
Cache MISS for query: SELECT * FROM orders...
Caching query result: 100 rows in 1 batch
Cache HIT for query: SELECT * FROM orders...
```

### Verify Query Normalization

**All these should hit the same cache entry**:

```sql
-- Query 1
SELECT * FROM orders WHERE status = 'shipped'

-- Query 2 (extra spaces)
SELECT   *   FROM   orders   WHERE   status   =   'shipped'

-- Query 3 (different case)
select * from orders where status = 'shipped'

-- Query 4 (tabs and newlines)
SELECT * 
FROM orders 
WHERE status = 'shipped'
```

**Verification**:
- First query: Cache MISS (slow)
- Queries 2-4: Cache HIT (fast)

### Verify TTL Expiration

**Test cache expiration**:

```bash
# Set short TTL for testing
export CUBESQL_QUERY_CACHE_TTL=10  # 10 seconds
./start-cubesqld.sh

# Run query
psql -h 127.0.0.1 -p 4444 -U username -c "SELECT * FROM orders LIMIT 10"
# Time: 800ms (cache MISS)

# Run immediately (cache HIT)
psql -h 127.0.0.1 -p 4444 -U username -c "SELECT * FROM orders LIMIT 10"
# Time: 150ms (cache HIT)

# Wait 11 seconds
sleep 11

# Run again (cache MISS - expired)
psql -h 127.0.0.1 -p 4444 -U username -c "SELECT * FROM orders LIMIT 10"
# Time: 800ms (cache MISS)
```

### Verify Cache Disabled

**Test with cache disabled**:

```bash
export CUBESQL_QUERY_CACHE_ENABLED=false
./start-cubesqld.sh

# Run same query twice
psql -h 127.0.0.1 -p 4444 -U username -c "SELECT * FROM orders LIMIT 100"
# Time: 800ms

psql -h 127.0.0.1 -p 4444 -U username -c "SELECT * FROM orders LIMIT 100"
# Time: 800ms (same - no cache!)
```

## Performance Benchmarking

### Automated Benchmark Script

```bash
cat > benchmark.sh << 'SCRIPT'
#!/bin/bash
echo "Running benchmark: Cache disabled vs enabled"
echo ""

# Test with cache disabled
export CUBESQL_QUERY_CACHE_ENABLED=false
./start-cubesqld.sh > /dev/null 2>&1 &
PID=$!
sleep 3

echo "Cache DISABLED:"
for i in {1..5}; do
  time psql -h 127.0.0.1 -p 4444 -U username -c \
    "SELECT * FROM orders_with_preagg LIMIT 500" > /dev/null 2>&1
done

kill $PID
sleep 2

# Test with cache enabled
export CUBESQL_QUERY_CACHE_ENABLED=true
./start-cubesqld.sh > /dev/null 2>&1 &
PID=$!
sleep 3

echo ""
echo "Cache ENABLED:"
for i in {1..5}; do
  time psql -h 127.0.0.1 -p 4444 -U username -c \
    "SELECT * FROM orders_with_preagg LIMIT 500" > /dev/null 2>&1
done

kill $PID
SCRIPT

chmod +x benchmark.sh
./benchmark.sh
```

**Expected output**:
```
Cache DISABLED:
real    0m1.200s
real    0m1.180s
real    0m1.220s
...

Cache ENABLED:
real    0m1.250s  (first - cache MISS)
real    0m0.200s  (cached!)
real    0m0.210s  (cached!)
...
```

## Verification Matrix

| Test | Expected Result | How to Verify |
|------|----------------|---------------|
| Code formatting | All files pass `cargo fmt --check` | Run in rust/cubesql |
| Linting | Zero clippy warnings | Run `cargo clippy -D warnings` |
| Unit tests | 5/5 passing | Run `cargo test arrow_native::cache` |
| Python tests | 4/4 passing, 8-15x speedup | Run test_arrow_native_performance.py |
| Cache hit | 3-10x faster on repeat query | Manual psql test |
| Query normalization | Whitespace/case ignored | Run similar queries |
| TTL expiration | Cache clears after TTL | Set short TTL, wait, test |
| Cache disabled | No speedup on repeat | Set ENABLED=false |
| Sample data | 3000 orders loaded | Run setup_test_data.sh |

## Common Issues and Solutions

### Issue: Python tests timeout

**Symptom**: Tests hang or timeout
**Solution**:
```bash
# Check CubeSQL is running
lsof -i:4444

# Check Cube API is running
lsof -i:4008

# Restart services
killall cubesqld node
./start-cube-api.sh &
./start-cubesqld.sh &
```

### Issue: Inconsistent performance

**Symptom**: Speedup varies widely
**Solution**:
```bash
# Warm up the system first
for i in {1..3}; do
  psql -h 127.0.0.1 -p 4444 -U username -c "SELECT 1" > /dev/null
done

# Then run actual tests
```

### Issue: Cache not visible in logs

**Symptom**: No cache messages in logs
**Solution**:
```bash
# Enable debug logging
export CUBESQL_LOG_LEVEL=debug
./start-cubesqld.sh

# Or check specific log file
tail -f cubesqld.log | grep -i "cache\|query result"
```

## Full PR Verification Workflow

**Complete end-to-end verification**:

```bash
# 1. Clean slate
cd /path/to/cube
git checkout feature/arrow-ipc-api
git pull
make clean || cargo clean

# 2. Build and test Rust
cd rust/cubesql
cargo fmt --all
cargo clippy --all -- -D warnings
cargo build --release
cargo test arrow_native::cache

# 3. Set up environment
cd ../../examples/recipes/arrow-ipc
docker-compose down
docker-compose up -d postgres
sleep 5
./setup_test_data.sh

# 4. Start services
./start-cube-api.sh > cube-api.log 2>&1 &
sleep 5
./start-cubesqld.sh > cubesqld.log 2>&1 &
sleep 3

# 5. Verify cache is enabled
grep "Query result cache: ENABLED" cubesqld.log

# 6. Run Python tests
python3 -m venv .venv
source .venv/bin/activate
pip install psycopg2-binary requests
python test_arrow_native_performance.py

# 7. Manual verification
psql -h 127.0.0.1 -p 4444 -U username << SQL
\timing on
SELECT * FROM orders_with_preagg LIMIT 100;
SELECT * FROM orders_with_preagg LIMIT 100;
SQL

# 8. Clean up
killall cubesqld node
docker-compose down
```

**Expected timeline**: 10-15 minutes for complete verification

## Success Criteria

✅ All checks passing:
- [x] Code formatted and linted
- [x] Release build successful
- [x] Unit tests passing
- [x] Sample data loaded (3000 orders)
- [x] Cache initialization confirmed in logs
- [x] Python tests show 8-15x average speedup
- [x] Manual psql tests show cache hits
- [x] Query normalization works
- [x] TTL expiration works
- [x] Cache can be disabled

**If all criteria met**: PR is ready for submission! 🎉
