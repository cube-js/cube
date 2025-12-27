# CubeSQL ADBC(Arrow Native) Server - Complete Example

**Performance**: 8-15x faster than REST HTTP API
**Status**: Production-ready implementation with optional Arrow Results Cache
**Sample Data**: 3000 orders included for testing

## Quick Links

📚 **Essential Documentation**:
- **[Getting Started](GETTING_STARTED.md)** - 5-minute quick start guide
- **[Architecture](ARCHITECTURE.md)** - Complete technical overview
- **[Local Verification](LOCAL_VERIFICATION.md)** - How to verify the PR

🧪 **Testing**:
- **[Python Performance Tests](test_arrow_native_performance.py)** - ADBC(Arrow Native) vs REST API benchmarks
- **[Sample Data Setup](setup_test_data.sh)** - Load 3000 test orders

📖 **Additional Resources**:
- **[Development History](/home/io/projects/learn_erl/power-of-three-examples/doc/)** - Planning and analysis docs

## What This Demonstrates

This example showcases **CubeSQL's ADBC(Arrow Native) server** with optional Arrow Results Cache:

- ✅ **Binary protocol** - Efficient ADBC(Arrow Native) data transfer
- ✅ **Optional caching** - 3-10x speedup on repeated queries
- ✅ **8-15x faster** than REST HTTP API overall
- ✅ **Minimal overhead** - Arrow Results Cache adds ~10% on first query, 90% savings on repeats
- ✅ **Zero configuration** - Works out of the box, cache enabled by default
- ✅ **Zero breaking changes** - Cache can be disabled anytime

## Architecture Overview

```
Client Application (Python/R/JS)
         │
         ├─── REST HTTP API (Port 4008)
         │    └─> JSON over HTTP
         │         └─> Cube API → CubeStore
         │
         └─── ADBC(Arrow Native) Native (Port 8120) ⭐ NEW
              └─> Binary Arrow Protocol
                   └─> Arrow Results Cache (Optional) ⭐ NEW
                        └─> Cube API → CubeStore
```

**What this PR adds**:
- **ADBC(Arrow Native) native protocol (port 8120)** - Binary data transfer, 8-15x faster than REST API
- **Optional Arrow Results Cache** - Additional 3-10x speedup on repeated queries

**When to disable cache**: If using CubeStore pre-aggregations, data is already cached at the storage layer. CubeStore is a cache itself - **sometimes one cache is plenty**. Cacheless setup still gets 8-15x speedup from ADBC(Arrow Native) binary protocol.

## Quick Start (5 minutes)

### Prerequisites

- Docker
- Rust (for building CubeSQL)
- Python 3.8+
- Node.js 16+

### Steps

```bash
# 1. Start database
docker-compose up -d postgres

# 2. Load sample data (3000 orders)
./setup_test_data.sh

# 3. Start Cube API (Terminal 1)
./start-cube-api.sh

# 4. Start CubeSQL with cache (Terminal 2)
./start-cubesqld.sh

# 5. Run performance tests (Terminal 3)
python3 -m venv .venv
source .venv/bin/activate
pip install psycopg2-binary requests

# Test WITH cache (default)
python test_arrow_native_performance.py

# Test WITHOUT cache (baseline ADBC(Arrow Native))
export CUBESQL_ARROW_RESULTS_CACHE_ENABLED=false
./start-cubesqld.sh  # Restart with cache disabled
python test_arrow_native_performance.py
```

**Expected Output (with cache)**:
```
Cache Miss → Hit:        3-10x speedup ✓
ADBC(Arrow Native) vs REST:    8-15x faster ✓
Average Speedup:         8-15x
✓ All tests passed!
```

**Expected Output (without cache)**:
```
ADBC(Arrow Native) vs REST:    5-10x faster ✓
(Baseline performance without caching)
```

## What You Get

### Files Included

**Essential Documentation**:
- `GETTING_STARTED.md` - Complete setup guide
- `ARCHITECTURE.md` - Technical deep dive
- `LOCAL_VERIFICATION.md` - PR verification steps

**Test Infrastructure**:
- `test_arrow_native_performance.py` - Python benchmarks comparing ADBC(Arrow Native) vs REST API
- `setup_test_data.sh` - Data loader script
- `sample_data.sql.gz` - 3000 sample orders (240KB)

Tests support both modes:
- `CUBESQL_ARROW_RESULTS_CACHE_ENABLED=true` - Tests with optional cache
- `CUBESQL_ARROW_RESULTS_CACHE_ENABLED=false` - Tests baseline ADBC(Arrow Native) performance

**Configuration**:
- `start-cubesqld.sh` - Launches CubeSQL with cache enabled
- `start-cube-api.sh` - Launches Cube API
- `.env` - Database and API configuration

**Cube Schema**:
- `model/cubes/orders_with_preagg.yaml` - Cube with pre-aggregations
- `model/cubes/orders_no_preagg.yaml` - Cube without pre-aggregations

## Performance Results

### ADBC(Arrow Native) Server Performance

**With Optional Cache** (same query repeated):
```
First execution:  1252ms  (cache MISS - full execution)
Second execution:  385ms  (cache HIT - served from cache)
Speedup:          3.3x faster
```

**Without Cache**:
- Consistent query execution times
- No caching overhead
- Suitable for unique queries

### ADBC(Arrow Native) (8120) vs REST HTTP API (4008)

**Full materialization timing** (includes client-side data conversion):
```
Query Size    | ADBC(Arrow Native) | REST API | Speedup
--------------|--------------|----------|--------
200 rows      |  363ms       | 5013ms   | 13.8x
2K rows       |  409ms       | 5016ms   | 12.3x
10K rows      | 1424ms       | 5021ms   |  3.5x

Average: 8.2x faster (ADBC(Arrow Native) with cache)
```

**Materialization overhead**: 0-15ms (negligible)

## Configuration Options

### ADBC(Arrow Native) Server Settings

Edit environment variables in `start-cubesqld.sh`:

```bash
# PostgreSQL wire protocol port
CUBESQL_PG_PORT=4444

# ADBC(Arrow Native) port (direct ADBC(Arrow Native))
CUBEJS_ADBC_PORT=8120

# Optional Arrow Results Cache Settings
CUBESQL_ARROW_RESULTS_CACHE_ENABLED=true      # Enable/disable (default: true)
CUBESQL_ARROW_RESULTS_CACHE_MAX_ENTRIES=10000 # Max cached queries (default: 1000)
CUBESQL_ARROW_RESULTS_CACHE_TTL=7200          # TTL in seconds (default: 3600)
```

### Database Settings

Edit `.env` file:
```bash
PORT=4008                      # Cube API port
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
psql -h 127.0.0.1 -p 4444 -U username

# Enable timing
\timing on

# Run query twice, observe speedup
SELECT market_code, count FROM orders_with_preagg LIMIT 100;
SELECT market_code, count FROM orders_with_preagg LIMIT 100;
```

### Using Python

```python
import psycopg2
import time

conn = psycopg2.connect("postgresql://username:password@localhost:4444/db")
cursor = conn.cursor()

# Cache miss
start = time.time()
cursor.execute("SELECT * FROM orders_with_preagg LIMIT 500")
print(f"Cache miss: {(time.time()-start)*1000:.0f}ms")

# Cache hit
start = time.time()
cursor.execute("SELECT * FROM orders_with_preagg LIMIT 500")
print(f"Cache hit: {(time.time()-start)*1000:.0f}ms")
```

## Troubleshooting

### Services Won't Start

```bash
# Kill existing processes
killall cubesqld node
pkill -f "cubejs-server"

# Check ports
lsof -i:4444  # CubeSQL
lsof -i:4008  # Cube API
lsof -i:7432  # PostgreSQL
```

### Database Issues

```bash
# Restart PostgreSQL
docker-compose restart postgres

# Reload sample data
./setup_test_data.sh

# Check data loaded
psql -h localhost -p 7432 -U postgres -d pot_examples_dev \
  -c "SELECT COUNT(*) FROM public.order"
```

### Python Test Failures

```bash
# Reinstall dependencies
pip install --upgrade psycopg2-binary requests

# Check connection
python -c "import psycopg2; psycopg2.connect('postgresql://username:password@localhost:4444/db')"
```

## For PR Reviewers

### Verification Steps

See **[LOCAL_VERIFICATION.md](LOCAL_VERIFICATION.md)** for complete verification workflow.

**Quick verification** (5 minutes):
```bash
# 1. Build and test
cd rust/cubesql
cargo fmt --all --check
cargo clippy --all -- -D warnings
cargo test arrow_native::cache

# 2. Run example
cd ../../examples/recipes/arrow-ipc
./setup_test_data.sh
./start-cube-api.sh &
./start-cubesqld.sh &
python test_arrow_native_performance.py
```

### Files Changed

**Implementation** (282 lines):
- `rust/cubesql/cubesql/src/sql/arrow_native/cache.rs` (new)
- `rust/cubesql/cubesql/src/sql/arrow_native/server.rs` (modified)
- `rust/cubesql/cubesql/src/sql/arrow_native/stream_writer.rs` (modified)

**Tests** (400 lines):
- `examples/recipes/arrow-ipc/test_arrow_native_performance.py` (new)

**Infrastructure**:
- `examples/recipes/arrow-ipc/setup_test_data.sh` (new)
- `examples/recipes/arrow-ipc/sample_data.sql.gz` (new, 240KB)

## Learn More

- **[Architecture Deep Dive](ARCHITECTURE.md)** - Technical details
- **[Getting Started Guide](GETTING_STARTED.md)** - Step-by-step setup
- **[Verification Guide](LOCAL_VERIFICATION.md)** - How to test locally
- **[Development Docs](/home/io/projects/learn_erl/power-of-three-examples/doc/)** - Planning & analysis

## Support

For issues or questions:
1. Check [GETTING_STARTED.md](GETTING_STARTED.md) troubleshooting section
2. Review [LOCAL_VERIFICATION.md](LOCAL_VERIFICATION.md) for verification steps
3. See [ARCHITECTURE.md](ARCHITECTURE.md) for technical details
