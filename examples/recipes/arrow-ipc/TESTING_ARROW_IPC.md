# Testing Arrow IPC Feature in CubeSQL

## Build Status

✅ **Build Successful**

The CubeSQL binary has been built in release mode:
```
Location: /home/io/projects/learn_erl/cube/rust/cubesql/target/release/cubesqld
Size: 44MB (optimized release build)
```

## Starting CubeSQL Server

### Option 1: With Cube.js Backend (Full Testing)

If you have a Cube.js instance running:

```bash
# Set your Cube.js credentials and start CubeSQL
export CUBESQL_CUBE_URL=https://your-cube-instance.com/cubejs-api
export CUBESQL_CUBE_TOKEN=your-api-token
export CUBESQL_LOG_LEVEL=debug
export CUBESQL_BIND_ADDR=0.0.0.0:4444

# Start the server
/home/io/projects/learn_erl/cube/rust/cubesql/target/release/cubesqld
```

Server will listen on `127.0.0.1:4444`

### Option 2: Local Testing Without Backend

For testing the Arrow IPC protocol layer without a Cube.js backend:

```bash
# Just start the server with minimal config
CUBESQL_LOG_LEVEL=debug \
/home/io/projects/learn_erl/cube/rust/cubesql/target/release/cubesqld
```

This will allow you to test system catalog queries which don't require a backend.

## Testing Arrow IPC Feature

### 1. Basic Connection Test

```bash
# In another terminal, connect with psql
psql -h 127.0.0.1 -p 4444 -U root
```

Once connected:
```sql
-- Check that we're connected
SELECT version();

-- Check current output format (should be 'postgresql')
SHOW output_format;
```

### 2. Enable Arrow IPC Output

```sql
-- Set output format to Arrow IPC
SET output_format = 'arrow_ipc';

-- Verify it was set
SHOW output_format;
```

Expected output: `arrow_ipc`

### 3. Test with System Queries

```sql
-- Query system tables (these work without Cube backend)
SELECT * FROM information_schema.tables LIMIT 5;

SELECT * FROM information_schema.columns LIMIT 10;

SELECT * FROM pg_catalog.pg_tables LIMIT 5;
```

When Arrow IPC is enabled, the response format changes from PostgreSQL wire protocol to Apache Arrow IPC streaming format. The psql client should still display results (with some conversion overhead).

### 4. Test Format Switching

```sql
-- Switch back to PostgreSQL format
SET output_format = 'postgresql';

-- Run a query
SELECT 1 as test_value;

-- Switch to Arrow IPC again
SET output_format = 'arrow_ipc';

-- Run another query
SELECT 2 as test_value;

-- Back to PostgreSQL
SET output_format = 'postgresql';

SELECT 3 as test_value;
```

### 5. Test Invalid Format

```sql
-- This should fail or be rejected
SET output_format = 'invalid_format';
```

### 6. Test Format Persistence

```sql
SET output_format = 'arrow_ipc';

-- Run multiple queries
SELECT 1 as num1;
SELECT 2 as num2;
SELECT 3 as num3;

-- Format should persist across all queries
```

## Client Library Testing

### Python Client

**Prerequisites:**
```bash
pip install psycopg2-binary pyarrow pandas
```

**Test Script:**
```python
from examples.arrow_ipc_client import CubeSQLArrowIPCClient

client = CubeSQLArrowIPCClient(host="127.0.0.1", port=4444)

try:
    client.connect()
    print("✓ Connected to CubeSQL")

    client.set_arrow_ipc_output()
    print("✓ Set Arrow IPC output format")

    # Test with system tables
    result = client.execute_query_with_arrow_streaming(
        "SELECT * FROM information_schema.tables LIMIT 5"
    )
    print(f"✓ Retrieved {len(result)} rows")
    print("\nFirst row:")
    print(result.iloc[0] if len(result) > 0 else "No data")

except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()
finally:
    client.close()
```

Save as `test_arrow_ipc.py` and run:
```bash
cd /home/io/projects/learn_erl/cube
python test_arrow_ipc.py
```

### JavaScript Client

**Prerequisites:**
```bash
npm install pg apache-arrow
```

**Test Script:**
```javascript
const { CubeSQLArrowIPCClient } = require("./examples/arrow_ipc_client.js");

async function test() {
  const client = new CubeSQLArrowIPCClient({
    host: "127.0.0.1",
    port: 4444,
    user: "root"
  });

  try {
    await client.connect();
    console.log("✓ Connected to CubeSQL");

    await client.setArrowIPCOutput();
    console.log("✓ Set Arrow IPC output format");

    const result = await client.executeQuery(
      "SELECT * FROM information_schema.tables LIMIT 5"
    );
    console.log(`✓ Retrieved ${result.length} rows`);
    console.log("\nFirst row:");
    console.log(result[0]);

  } catch (error) {
    console.error(`✗ Error: ${error.message}`);
  } finally {
    await client.close();
  }
}

test();
```

Save as `test_arrow_ipc.js` and run:
```bash
cd /home/io/projects/learn_erl/cube
node test_arrow_ipc.js
```

### R Client

**Prerequisites:**
```r
install.packages(c("RPostgres", "arrow", "tidyverse", "dplyr", "R6"))
```

**Test Script:**
```r
source("examples/arrow_ipc_client.R")

client <- CubeSQLArrowIPCClient$new(
  host = "127.0.0.1",
  port = 4444L,
  user = "root"
)

tryCatch({
  client$connect()
  cat("✓ Connected to CubeSQL\n")

  client$set_arrow_ipc_output()
  cat("✓ Set Arrow IPC output format\n")

  result <- client$execute_query(
    "SELECT * FROM information_schema.tables LIMIT 5"
  )
  cat(sprintf("✓ Retrieved %d rows\n", nrow(result)))
  cat("\nFirst row:\n")
  print(head(result, 1))

}, error = function(e) {
  cat(sprintf("✗ Error: %s\n", e$message))
}, finally = {
  client$close()
})
```

Save as `test_arrow_ipc.R` and run:
```r
source("test_arrow_ipc.R")
```

## Monitoring Server Logs

To see detailed logs while testing:

```bash
# Terminal 1: Start server with debug logging
CUBESQL_LOG_LEVEL=debug \
/home/io/projects/learn_erl/cube/rust/cubesql/target/release/cubesqld

# Terminal 2: Run client tests
python test_arrow_ipc.py
```

Look for log messages indicating:
- `SET output_format = 'arrow_ipc'`
- Query execution with format branching
- Arrow IPC serialization

## Expected Behavior

### With Arrow IPC Enabled

1. **Query Execution**: Queries should execute successfully
2. **Response Format**: Results are in Arrow IPC binary format
3. **Data Integrity**: All column data should be preserved
4. **Format Persistence**: Format setting persists across queries in same session

### PostgreSQL Format (Default)

1. **Query Execution**: Queries work normally
2. **Response Format**: PostgreSQL wire protocol format
3. **Backward Compatibility**: Existing clients work unchanged

## Performance Testing

Compare performance with and without Arrow IPC:

```python
import time
from examples.arrow_ipc_client import CubeSQLArrowIPCClient

client = CubeSQLArrowIPCClient()
client.connect()

# Test 1: PostgreSQL format (default)
print("PostgreSQL format (default):")
start = time.time()
for i in range(10):
    result = client.execute_query_with_arrow_streaming(
        "SELECT * FROM information_schema.columns LIMIT 100"
    )
pg_time = time.time() - start
print(f"  10 queries: {pg_time:.3f}s")

# Test 2: Arrow IPC format
print("\nArrow IPC format:")
client.set_arrow_ipc_output()
start = time.time()
for i in range(10):
    result = client.execute_query_with_arrow_streaming(
        "SELECT * FROM information_schema.columns LIMIT 100"
    )
arrow_time = time.time() - start
print(f"  10 queries: {arrow_time:.3f}s")

# Compare
if arrow_time > 0:
    speedup = pg_time / arrow_time
    print(f"\nSpeedup: {speedup:.2f}x")

client.close()
```

## Running Integration Tests

If you have a Cube.js instance configured:

```bash
cd /home/io/projects/learn_erl/cube/rust/cubesql

# Set environment variables
export CUBESQL_TESTING_CUBE_TOKEN=your-token
export CUBESQL_TESTING_CUBE_URL=your-url

# Run integration tests
cargo test --test arrow_ipc 2>&1 | tail -50
```

## Troubleshooting

### Connection Refused
```
Error: Failed to connect to CubeSQL
Solution: Ensure cubesqld is running and listening on 127.0.0.1:4444
```

### Format Not Changing
```sql
-- Verify exact syntax with quotes
SET output_format = 'arrow_ipc';
-- Valid values: 'postgresql', 'postgres', 'pg', 'arrow_ipc', 'arrow', 'ipc'
```

### Python Import Error
```bash
# Install missing packages
pip install psycopg2-binary pyarrow pandas
```

### JavaScript Module Not Found
```bash
# Install dependencies
npm install pg apache-arrow
```

### Queries Return No Data
Check that:
1. CubeSQL is properly configured with Cube.js backend
2. System tables are accessible (`SELECT * FROM information_schema.tables`)
3. No errors in server logs

## Next Steps

1. **Basic Protocol Testing**: Start with system table queries
2. **Client Testing**: Test each client library (Python, JavaScript, R)
3. **Performance Benchmarking**: Compare with/without Arrow IPC
4. **Integration Testing**: Test with real Cube.js instance
5. **BI Tool Testing**: Test with Tableau, Metabase, etc.

## Support

For issues or questions:
1. Check server logs: `CUBESQL_LOG_LEVEL=debug`
2. Review `examples/ARROW_IPC_GUIDE.md` for detailed documentation
3. Check `PHASE_3_SUMMARY.md` for implementation details
4. Review test code in `cubesql/e2e/tests/arrow_ipc.rs`
