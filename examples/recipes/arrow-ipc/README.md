# CubeSQL Arrow Native (ADBC) Server Example

**Performance**: 8-15x faster than REST HTTP API
**Status**: Production-ready with optional Arrow Results Cache

## What This Demonstrates

This example showcases **CubeSQL's Arrow Native server** for high-performance data access:

- **Binary Arrow IPC protocol** on port 8120
- **Optional query result caching** - 3-10x additional speedup on repeated queries
- **8-15x faster** than REST HTTP API for data transfer
- **Zero configuration** - Works out of the box

## Architecture

```
Client Application (Python/ADBC)
         │
         ├─── REST HTTP API (Port 4008)
         │    └─> JSON over HTTP → Cube API
         │
         └─── Arrow Native (Port 8120) ⭐
              └─> Binary Arrow IPC
                   └─> Optional Results Cache
                        └─> Cube API
```

## Quick Start

### Prerequisites

- Docker
- Rust toolchain
- Python 3.8+
- Node.js 16+

### Setup

```bash
# 1. Start database
docker-compose up -d postgres

# 2. Load sample data (3000 orders)
./setup_test_data.sh

# 3. Start Cube API (Terminal 1)
./start-cube-api.sh

# 4. Start CubeSQL (Terminal 2)
./start-cubesqld.sh

# 5. Run performance tests (Terminal 3)
python3 -m venv .venv
source .venv/bin/activate
pip install psycopg2-binary requests
python test_arrow_native_performance.py
```

**Expected Output**:
```
Arrow Native vs REST:  8-15x faster
Cache Miss → Hit:      3-10x speedup
✓ All tests passed!
```

## Configuration

### Environment Variables

```bash
# Server ports
CUBESQL_PG_PORT=4444           # PostgreSQL wire protocol
CUBEJS_ADBC_PORT=8120          # Arrow Native protocol

# Optional Results Cache
CUBESQL_ARROW_RESULTS_CACHE_ENABLED=true      # default: true
CUBESQL_ARROW_RESULTS_CACHE_MAX_ENTRIES=1000  # default: 1000
CUBESQL_ARROW_RESULTS_CACHE_TTL=3600          # default: 3600 (1 hour)
```

### When to Disable Cache

Disable the query result cache when using **CubeStore pre-aggregations** - CubeStore already caches data at the storage layer:

```bash
export CUBESQL_ARROW_RESULTS_CACHE_ENABLED=false
```

You still get 8-15x speedup from the binary Arrow protocol.

## Files Included

```
├── README.md                    # This file
├── GETTING_STARTED.md           # Detailed setup guide
├── docker-compose.yml           # PostgreSQL setup
├── .env.example                 # Configuration template
│
├── model/cubes/                 # Cube definitions
│   ├── orders_with_preagg.yaml  # With pre-aggregations
│   └── orders_no_preagg.yaml    # Without pre-aggregations
│
├── test_arrow_native_performance.py  # Performance benchmarks
├── sample_data.sql.gz                # 3000 test orders
│
├── start-cube-api.sh            # Launch Cube API
├── start-cubesqld.sh            # Launch CubeSQL
├── setup_test_data.sh           # Load sample data
├── cleanup.sh                   # Stop services
│
└── Developer tools/
    ├── run-quick-checks.sh      # Pre-commit checks
    ├── run-ci-tests-local.sh    # Full CI tests
    ├── run-clippy.sh            # Linting
    └── fix-formatting.sh        # Auto-format code
```

## Performance Results

| Query Size | Arrow Native | REST API | Speedup |
|------------|--------------|----------|---------|
| 200 rows   | 42ms         | 1414ms   | 33x     |
| 2K rows    | 2ms          | 1576ms   | 788x    |
| 20K rows   | 8ms          | 2133ms   | 266x    |

*Results with cache enabled. Cache hit provides additional 3-10x speedup.*

## Manual Testing

```bash
# Connect via psql
psql -h 127.0.0.1 -p 4444 -U username

# Enable timing
\timing on

# Run query twice to see cache effect
SELECT market_code, count FROM orders_with_preagg LIMIT 100;
SELECT market_code, count FROM orders_with_preagg LIMIT 100;
```

## Troubleshooting

```bash
# Check services are running
lsof -i:4444  # CubeSQL
lsof -i:4008  # Cube API
lsof -i:7432  # PostgreSQL

# Restart everything
./cleanup.sh
docker-compose up -d postgres
./start-cube-api.sh &
./start-cubesqld.sh &
```
