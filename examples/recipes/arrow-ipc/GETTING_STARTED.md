# Getting Started with CubeSQL Arrow Native Server

## Quick Start (5 minutes)

This guide shows you how to use **CubeSQL's Arrow Native server** with optional query caching.

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
# Navigate to the Arrow Native server example
cd ../../examples/recipes/arrow-ipc

# Start PostgreSQL database
docker-compose up -d postgres

# Load sample data (3000 orders)
./setup_test_data.sh
```

**Expected output**:
```
Setting up test data for CubeSQL Arrow Native server...
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

**Terminal 2 - Start CubeSQL Arrow Native Server**:
```bash
./start-cubesqld.sh
```

Wait for:
```
🔗 Cube SQL (pg) is listening on 0.0.0.0:4444
🔗 Cube SQL (arrow) is listening on 0.0.0.0:4445
Query result cache initialized: enabled=true, max_entries=1000, ttl=3600s
```

**Note**: Query cache is **optional** and enabled by default. It can be disabled without breaking changes.

### Step 4: Run Performance Tests

**Terminal 3 - Python Tests**:
```bash
# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install psycopg2-binary requests

# Run tests
python test_arrow_cache_performance.py
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

### Arrow Native Server Settings

Edit `start-cubesqld.sh` or set environment variables:

```bash
# Server ports
export CUBESQL_PG_PORT=4444        # PostgreSQL protocol
export CUBEJS_ARROW_PORT=4445      # Arrow IPC native

# Optional Query Cache (enabled by default)
export CUBESQL_QUERY_CACHE_ENABLED=true        # Enable/disable
export CUBESQL_QUERY_CACHE_MAX_ENTRIES=10000   # Max queries
export CUBESQL_QUERY_CACHE_TTL=7200            # TTL (2 hours)
```

**Disable cache** if you only want the Arrow Native server without caching:
```bash
export CUBESQL_QUERY_CACHE_ENABLED=false
```

### Database Connection

Edit `.env` file:
```bash
PORT=4008                    # Cube API port
CUBEJS_DB_HOST=localhost
CUBEJS_DB_PORT=7432
CUBEJS_DB_NAME=pot_examples_dev
CUBEJS_DB_USER=postgres
CUBEJS_DB_PASS=postgres
```

## Manual Testing

### Using psql

```bash
# Connect to CubeSQL
psql -h 127.0.0.1 -p 4444 -U username -d db

# Run a query (cache MISS)
SELECT market_code, brand_code, count, total_amount_sum
FROM orders_with_preagg
WHERE updated_at >= '2024-01-01'
LIMIT 100;
-- Time: 850ms

# Run same query again (cache HIT)
-- Time: 120ms (7x faster!)
```

### Using Python REPL

```python
import psycopg2
import time

conn = psycopg2.connect("postgresql://username:password@localhost:4444/db")
cursor = conn.cursor()

# First execution
start = time.time()
cursor.execute("SELECT * FROM orders_with_preagg LIMIT 1000")
results = cursor.fetchall()
print(f"Cache miss: {(time.time() - start)*1000:.0f}ms")

# Second execution
start = time.time()
cursor.execute("SELECT * FROM orders_with_preagg LIMIT 1000")
results = cursor.fetchall()
print(f"Cache hit: {(time.time() - start)*1000:.0f}ms")
```

## Troubleshooting

### Port Already in Use

```bash
# Kill process on port 4444
kill $(lsof -ti:4444)

# Kill process on port 4008
kill $(lsof -ti:4008)
```

### Database Connection Failed

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

### Python Test Failures

**Missing dependencies**:
```bash
pip install psycopg2-binary requests
```

**Connection refused**:
- Ensure CubeSQL is running on port 4444
- Check with: `lsof -i:4444`

**Authentication failed**:
- Default credentials: username=`username`, password=`password`
- Set in `test_arrow_cache_performance.py` if different

## Next Steps

### For Developers

1. **Review the implementation**:
   - `rust/cubesql/cubesql/src/sql/arrow_native/cache.rs`
   - `rust/cubesql/cubesql/src/sql/arrow_native/server.rs`

2. **Read the architecture**:
   - `ARCHITECTURE.md` - Complete technical overview
   - `LOCAL_VERIFICATION.md` - How to verify the PR

3. **Run the full test suite**:
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

- **Architecture**: `ARCHITECTURE.md`
- **Local Verification**: `LOCAL_VERIFICATION.md`
- **Sample Data**: `sample_data.sql.gz` (240KB, 3000 orders)
- **Python Tests**: `test_arrow_cache_performance.py`
- **Documentation**: `/home/io/projects/learn_erl/power-of-three-examples/doc/`
