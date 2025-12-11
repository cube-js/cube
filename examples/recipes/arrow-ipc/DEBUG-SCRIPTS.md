# Debug Scripts for Arrow Native Development

This directory contains separate scripts for debugging the Arrow Native protocol implementation.

## Available Scripts

### 1. `start-cube-api.sh`
Starts only the Cube.js API server (without protocol servers)

**What it does:**
- Starts Cube.js API on port 4008 (configurable via .env)
- Connects to PostgreSQL database
- **Disables** built-in PostgreSQL and Arrow Native protocol servers
- Logs output to `cube-api.log`

**Environment Variables Used:**
```bash
PORT=4008                           # Cube API HTTP port
CUBEJS_DB_TYPE=postgres            # Database type
CUBEJS_DB_HOST=localhost           # Database host
CUBEJS_DB_PORT=7432                # Database port
CUBEJS_DB_NAME=pot_examples_dev    # Database name
CUBEJS_DB_USER=postgres            # Database user
CUBEJS_DB_PASS=postgres            # Database password
CUBEJS_DEV_MODE=true               # Development mode
CUBEJS_LOG_LEVEL=trace             # Log level
```

**Usage:**
```bash
cd cube/examples/recipes/arrow-ipc
./start-cube-api.sh
```

**Expected Output:**
```
======================================
Cube.js API Server (Standalone)
======================================

Configuration:
  API Port: 4008
  API URL: http://localhost:4008/cubejs-api
  Database: postgres at localhost:7432
  Database Name: pot_examples_dev
  Log Level: trace

Note: PostgreSQL and Arrow Native protocols are DISABLED
      Use cubesqld for those (see start-cubesqld.sh)
```

**To Stop:**
Press `Ctrl+C`

---

### 2. `start-cubesqld.sh`
Starts the Rust cubesqld server with both PostgreSQL and Arrow Native protocols

**Prerequisites:**
- Cube.js API server must be running (start with `start-cube-api.sh` first)
- cubesqld binary must be built

**What it does:**
- Connects to Cube.js API on port 4008
- Starts PostgreSQL protocol on port 4444
- Starts Arrow Native protocol on port 4445
- Uses debug or release build automatically

**Environment Variables Used:**
```bash
CUBESQL_CUBE_URL=http://localhost:4008/cubejs-api   # Cube API endpoint
CUBESQL_CUBE_TOKEN=test                             # API token
CUBESQL_PG_PORT=4444                                # PostgreSQL port
CUBEJS_ARROW_PORT=4445                              # Arrow Native port
CUBESQL_LOG_LEVEL=info                              # Log level (info/debug/trace)
```

**Usage:**
```bash
cd cube/examples/recipes/arrow-ipc
./start-cubesqld.sh
```

**Expected Output:**
```
======================================
Cube SQL (cubesqld) Server
======================================

Found cubesqld binary (debug):
  rust/cubesql/target/debug/cubesqld

Configuration:
  Cube API URL: http://localhost:4008/cubejs-api
  Cube Token: test
  PostgreSQL Port: 4444
  Arrow Native Port: 4445
  Log Level: info

To test the connections:
  PostgreSQL: psql -h 127.0.0.1 -p 4444 -U root
  Arrow Native: Use ADBC driver with connection_mode=native

🔗 Cube SQL (pg) is listening on 0.0.0.0:4444
🔗 Cube SQL (arrow) is listening on 0.0.0.0:4445
```

**To Stop:**
Press `Ctrl+C`

---

## Complete Debugging Workflow

### Step 1: Build cubesqld (if not already built)
```bash
cd cube/rust/cubesql
cargo build --bin cubesqld
# Or for optimized build:
# cargo build --release --bin cubesqld
```

### Step 2: Start Cube.js API Server
```bash
# In terminal 1
cd cube/examples/recipes/arrow-ipc
./start-cube-api.sh
```

Wait for the message: `🚀 Cube API server is listening on 4008`

### Step 3: Start cubesqld Server
```bash
# In terminal 2
cd cube/examples/recipes/arrow-ipc
./start-cubesqld.sh
```

Wait for:
```
🔗 Cube SQL (pg) is listening on 0.0.0.0:4444
🔗 Cube SQL (arrow) is listening on 0.0.0.0:4445
```

### Step 4: Test the Connection

**Test with ADBC Python Client:**
```bash
# In terminal 3
cd adbc/python/adbc_driver_cube
source venv/bin/activate
python quick_test.py
```

**Expected result:**
```
✅ All checks PASSED!

Got 34 rows
Data: {'brand': ['Miller Draft', 'Patagonia', ...], ...}
```

**Test with PostgreSQL Client:**
```bash
psql -h 127.0.0.1 -p 4444 -U root
```

Then run queries:
```sql
SELECT * FROM of_customers LIMIT 10;
SELECT brand, MEASURE(count) FROM of_customers GROUP BY 1;
```

---

## Troubleshooting

### Port Already in Use
```bash
# Find what's using the port
lsof -i :4445

# Kill the process
kill $(lsof -ti:4445)
```

### Cube API Not Responding
Check logs:
```bash
tail -f cube/examples/recipes/arrow-ipc/cube-api.log
```

### cubesqld Not Building
```bash
cd cube/rust/cubesql
cargo clean
cargo build --bin cubesqld
```

### Database Connection Issues
Ensure PostgreSQL is running:
```bash
cd cube/examples/recipes/arrow-ipc
docker-compose up -d postgres
```

Check database:
```bash
psql -h localhost -p 7432 -U postgres -d pot_examples_dev
```

---

## Environment Variables Reference

### .env File Location
`cube/examples/recipes/arrow-ipc/.env`

### Required Variables
```bash
# Cube API
PORT=4008

# Database
CUBEJS_DB_TYPE=postgres
CUBEJS_DB_HOST=localhost
CUBEJS_DB_PORT=7432
CUBEJS_DB_NAME=pot_examples_dev
CUBEJS_DB_USER=postgres
CUBEJS_DB_PASS=postgres

# Development
CUBEJS_DEV_MODE=true
CUBEJS_LOG_LEVEL=trace
NODE_ENV=development

# cubesqld Token (optional, defaults to 'test')
CUBESQL_CUBE_TOKEN=test

# Protocol Ports (DO NOT set these in .env when using separate scripts)
# CUBEJS_PG_SQL_PORT=4444    # Commented out - cubesqld handles this
# CUBEJS_ARROW_PORT=4445     # Commented out - cubesqld handles this
```

### Log Levels
- `error` - Only errors
- `warn` - Warnings and errors
- `info` - Info, warnings, and errors (default for cubesqld)
- `debug` - Debug messages + above
- `trace` - Very verbose, all messages (recommended for Cube API during development)

---

## Comparison: dev-start.sh vs Separate Scripts

### `dev-start.sh` (All-in-One)
**Pros:**
- Single command starts everything
- Automatic setup and configuration
- Good for production-like testing

**Cons:**
- Harder to debug individual components
- Must rebuild cubesqld every time (slow)
- Can't easily restart just one component

### Separate Scripts (start-cube-api.sh + start-cubesqld.sh)
**Pros:**
- Start components independently
- Faster iteration (rebuild only cubesqld)
- Easier to see logs from each component
- Better for development and debugging

**Cons:**
- Must manage two processes
- Need to start in correct order

**Recommendation:** Use separate scripts for development/debugging, use `dev-start.sh` for demos or integration testing.

---

## Quick Reference Commands

```bash
# Start everything (all-in-one)
./dev-start.sh

# Or start separately for debugging:
./start-cube-api.sh     # Terminal 1
./start-cubesqld.sh     # Terminal 2

# Test
cd adbc/python/adbc_driver_cube
source venv/bin/activate
python quick_test.py

# Monitor logs
tail -f cube-api.log                    # Cube API logs
# cubesqld logs go to stdout

# Stop everything
# Ctrl+C in each terminal
# Or:
pkill -f "yarn dev"
pkill cubesqld

# Check what's running
lsof -i :4008    # Cube API
lsof -i :4444    # PostgreSQL protocol
lsof -i :4445    # Arrow Native protocol
```

---

## Files Modified for Separate Script Support

**`.env`** - Commented out protocol ports:
```bash
# CUBEJS_PG_SQL_PORT=4444  # Disabled - using Rust cubesqld instead
# CUBEJS_ARROW_PORT=4445   # Disabled - using Rust cubesqld instead
```

This prevents Node.js from starting built-in protocol servers, allowing cubesqld to use those ports instead.

---

## Testing the Fix

After starting both servers, verify the Arrow Native protocol fix is working:

```bash
cd adbc/python/adbc_driver_cube
source venv/bin/activate

# Test real Cube query
python quick_test.py

# Or test specific query
python test_cube_query.py
```

Expected result should show 34 rows from the `of_customers` cube without any "Table not found" errors.

---

## Additional Resources

- **Main Project README:** `cube/rust/cubesql/README.md`
- **CLAUDE Guide:** `cube/rust/cubesql/CLAUDE.md`
- **Change Log:** `cube/rust/cubesql/change.log`
- **Original Script:** `./dev-start.sh`
