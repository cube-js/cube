# Getting Started with CubeSQL ADBC(Arrow Native) Server

## Quick Start (5 minutes)

This guide shows you how to use **CubeSQL's ADBC(Arrow Native) server** with optional Arrow Results Cache.

### Prerequisites

- Docker (for PostgreSQL)
- Rust toolchain (for building CubeSQL)
- Python 3.8+ (for running tests)
- Node.js 16+ (for Cube API)

### Step 1: Clone and Build

```bash
# Clone the repository
git clone https://github.com/cube-js/cube.git
cd cube
git checkout feature/arrow-ipc-api

# Build CubeSQL with cache support
cd rust/cubesql
cargo build --release

# Verify the binary
./target/release/cubesqld --version
```

### Step 2: Set Up Test Environment

```bash
# Navigate to the ADBC(Arrow Native) server example
cd ../../examples/recipes/arrow-ipc

# Start PostgreSQL database
docker-compose up -d postgres

# Load sample data (3000 orders)
./setup_test_data.sh
```

**Expected output**:
```
Setting up test data for CubeSQL ADBC(Arrow Native) server...
Database connection:
  Host: localhost
  Port: 7432
  ...
✓ Database ready with 3000 orders
```

### Step 3: Start Services

**Terminal 1 - Start Cube API**:
```bash
./start-cube-api.sh
```

Wait for:
```
🚀 Cube API server is listening on port 4008
```

**Terminal 2 - Start CubeSQL ADBC(Arrow Native) Server**:
```bash
./start-cubesqld.sh
```

Wait for:
```
🔗 Cube SQL (pg) is listening on 0.0.0.0:4444
🔗 Cube SQL (arrow) is listening on 0.0.0.0:8120
Arrow Results Cache initialized: enabled=true, max_entries=1000, ttl=3600s
```

**Note**: Arrow Results Cache is **optional** and enabled by default. It can be disabled without breaking changes.

### Step 4: Run Performance Tests

**Terminal 3 - Python Tests**:
```bash
# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install psycopg2-binary requests

# Run tests
python test_arrow_native_performance.py
```

**Expected results**:
```
Cache Miss → Hit:        3-10x speedup
CubeSQL vs REST API:     8-15x faster
```

## Understanding the Results

### What Gets Measured

The Python tests measure **full end-to-end performance**:
1. Query execution time
2. Client-side materialization time (converting to usable format)
3. Total time (query + materialization)

### Interpreting Output

```
CUBESQL | Query: 1252ms | Materialize:   0ms | Total: 1252ms |    500 rows
```

- **Query**: Time from SQL execution to receiving last batch
- **Materialize**: Time to convert results to Python dict format
- **Total**: Complete client experience

### Cache Hit vs Miss

**First query (cache MISS)**:
```
Query:        1252ms    ← Full execution
Materialize:     0ms
TOTAL:        1252ms
```

**Second query (cache HIT)**:
```
Query:         385ms    ← Served from cache
Materialize:     0ms
TOTAL:         385ms    ← 3.3x faster!
```

## Configuration Options

### ADBC(Arrow Native) Server Settings

Edit `start-cubesqld.sh` or set environment variables:

```bash
# Server ports
export CUBESQL_PG_PORT=4444        # PostgreSQL protocol
export CUBEJS_ADBC_PORT=8120      # ADBC(Arrow Native) native

# Optional Query Cache (enabled by default)
export CUBESQL_QUERY_CACHE_ENABLED=true        # Enable/disable
export CUBESQL_QUERY_CACHE_MAX_ENTRIES=10000   # Max queries
export CUBESQL_QUERY_CACHE_TTL=7200            # TTL (2 hours)
```

**When to disable cache**:
```bash
export CUBESQL_QUERY_CACHE_ENABLED=false
```

Disable query result cache when using **CubeStore pre-aggregations**. CubeStore is already a cache/pre-aggregation layer at the storage level - **sometimes one cache is plenty**. Benefits:
- Avoids double-caching overhead
- Reduces memory usage
- Simpler architecture (single caching layer)
- **Still gets 8-15x speedup** from ADBC(Arrow Native) binary protocol vs REST API

**Verification**: Check logs for `"Query result cache: DISABLED (using ADBC(Arrow Native) baseline performance)"`. Cache operations are completely bypassed when disabled.


```bash
# Check PostgreSQL is running
docker ps | grep postgres

# Restart database
docker-compose restart postgres

# Check connection manually
psql -h localhost -p 7432 -U postgres -d pot_examples_dev
```

### Cache Not Working

Check CubeSQL logs for:
```
Query result cache initialized: enabled=true, max_entries=1000, ttl=3600s
```

If cache is disabled:
```bash
export CUBESQL_QUERY_CACHE_ENABLED=true
./start-cubesqld.sh
```

## Next Steps

### For Developers

1. **Review the implementation**:
   - `rust/cubesql/cubesql/src/sql/arrow_native/cache.rs`
   - `rust/cubesql/cubesql/src/sql/arrow_native/server.rs`

2. **Run the full test suite**:
   ```bash
   cd rust/cubesql
   cargo test arrow_native::cache
   ```

### For Users

1. **Try with your own data**:
   - Modify cube schema in `model/cubes/`
   - Point to your database in `.env`
   - Run your queries

2. **Benchmark your workload**:
   - Use the Python test as a template
   - Measure cache effectiveness for your queries
   - Tune cache parameters

3. **Deploy to production**:
   - Build release binary: `cargo build --release`
   - Configure cache for your traffic
   - Monitor performance improvements

## Resources

- **Sample Data**: `sample_data.sql.gz` (240KB, 3000 orders)
- **Python Tests**: `test_arrow_native_performance.py`
- **Cube Schemas**: `model/cubes/`
