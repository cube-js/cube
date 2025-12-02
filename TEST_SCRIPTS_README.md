# Arrow IPC Testing Scripts

Two comprehensive testing scripts have been created to test the Arrow IPC feature in CubeSQL.

## Quick Start

### Start CubeSQL Server
```bash
cd /home/io/projects/learn_erl/cube
CUBESQL_LOG_LEVEL=debug ./rust/cubesql/target/release/cubesqld
```

### Run Tests (in another terminal)

**Option 1: Using psql (Recommended)**
```bash
cd /home/io/projects/learn_erl/cube
./test_arrow_ipc.sh
```

**Option 2: Using PostgreSQL Protocol**
```bash
cd /home/io/projects/learn_erl/cube
./test_arrow_ipc_curl.sh
```

## Test Script Details

### 1. test_arrow_ipc.sh
**Purpose**: Comprehensive testing using psql client

**What it tests**:
- ✅ Server connectivity
- ✅ Default format is 'postgresql'
- ✅ SET output_format = 'arrow_ipc' works
- ✅ Format shows as 'arrow_ipc' after SET
- ✅ Queries return data with Arrow IPC enabled
- ✅ Format switching (between arrow_ipc and postgresql)
- ✅ Invalid format handling
- ✅ System tables work with Arrow IPC
- ✅ Concurrent queries work

**Usage**:
```bash
# Run all tests (default)
./test_arrow_ipc.sh

# Quick tests only
./test_arrow_ipc.sh --quick

# Custom host/port
./test_arrow_ipc.sh --host 192.168.1.10 --port 5432

# Custom user
./test_arrow_ipc.sh --user myuser

# Get help
./test_arrow_ipc.sh --help
```

**Expected Output**:
```
═══════════════════════════════════════════════════════════════
Arrow IPC Feature Testing
═══════════════════════════════════════════════════════════════

ℹ Testing CubeSQL Arrow IPC output format
ℹ Target: 127.0.0.1:4444

Testing: Check if CubeSQL is running
✓ CubeSQL is running on 127.0.0.1:4444

Testing: Basic connection
✓ Connected to CubeSQL

Testing: Check default output format
✓ Default format is 'postgresql'

Testing: Set output format to 'arrow_ipc'
✓ SET output_format succeeded

Testing: Verify output format is 'arrow_ipc'
✓ Output format is now 'arrow_ipc'

Testing: Execute query with Arrow IPC format
✓ Query with Arrow IPC returned data (10 lines)

... (more tests)

═══════════════════════════════════════════════════════════════
Test Results Summary
═══════════════════════════════════════════════════════════════
Passed: 9
Failed: 0
Total: 9

✓ All tests passed!
```

### 2. test_arrow_ipc_curl.sh
**Purpose**: Protocol-level testing using PostgreSQL wire protocol

**What it tests**:
- ✅ TCP connection to PostgreSQL port
- ✅ Arrow IPC format via protocol
- ✅ Format switching in protocol
- ✅ Concurrent connections
- ✅ Large result sets
- ✅ Various SQL statement types

**Usage**:
```bash
# Run all tests (default)
./test_arrow_ipc_curl.sh

# Quick tests only
./test_arrow_ipc_curl.sh --quick

# Custom host/port
./test_arrow_ipc_curl.sh --host 192.168.1.10 --port 5432

# Show protocol documentation
./test_arrow_ipc_curl.sh --docs

# Get help
./test_arrow_ipc_curl.sh --help
```

**Expected Output**:
```
═══════════════════════════════════════════════════════════════
Arrow IPC PostgreSQL Protocol Testing
═══════════════════════════════════════════════════════════════

ℹ Testing CubeSQL Arrow IPC feature at protocol level
ℹ Target: 127.0.0.1:4444

Testing: Check if CubeSQL is running
✓ CubeSQL is listening on 127.0.0.1:4444

Testing: Raw TCP Connection to PostgreSQL Protocol Server
✓ TCP connection established

Testing: Arrow IPC Format via PostgreSQL Protocol
ℹ 1. Check default format is 'postgresql'
✓ Default format is 'postgresql'

ℹ 2. Set output format to 'arrow_ipc'
✓ SET command executed

ℹ 3. Verify format is now 'arrow_ipc'
✓ Format is now 'arrow_ipc'

... (more tests)

═══════════════════════════════════════════════════════════════
Testing Complete
═══════════════════════════════════════════════════════════════
✓ Arrow IPC feature testing finished
```

## Troubleshooting

### "CubeSQL is NOT running"
```bash
# Make sure server is started in another terminal
./rust/cubesql/target/release/cubesqld

# Check if port is listening
lsof -i :4444
# or
netstat -tulpn | grep 4444
```

### "Connection refused"
```bash
# Port may be in use, start on different port
CUBESQL_BIND_ADDR=0.0.0.0:5555 ./rust/cubesql/target/release/cubesqld

# Then test with custom port
./test_arrow_ipc.sh --port 5555
```

### "psql: command not found"
```bash
# Install PostgreSQL client
# Ubuntu/Debian:
sudo apt-get install postgresql-client

# macOS:
brew install postgresql

# Then retry tests
./test_arrow_ipc.sh
```

### "nc: command not found"
```bash
# Install netcat
# Ubuntu/Debian:
sudo apt-get install netcat-openbsd

# macOS:
brew install netcat

# Then retry tests
./test_arrow_ipc_curl.sh
```

## Test Scenarios

### Scenario 1: Basic Arrow IPC (5 minutes)
```bash
# Terminal 1: Start server
./rust/cubesql/target/release/cubesqld

# Terminal 2: Run quick tests
./test_arrow_ipc.sh --quick
```

### Scenario 2: Format Switching (10 minutes)
```bash
# Test format persistence and switching
./test_arrow_ipc.sh
```

### Scenario 3: Protocol Level (15 minutes)
```bash
# Test at PostgreSQL protocol level
./test_arrow_ipc_curl.sh --comprehensive
```

### Scenario 4: Client Library Testing (30 minutes)
```bash
# Test with Python client
pip install psycopg2-binary pyarrow pandas
python examples/arrow_ipc_client.py

# Test with JavaScript
npm install pg apache-arrow
node examples/arrow_ipc_client.js

# Test with R
Rscript -e "source('examples/arrow_ipc_client.R'); run_all_examples()"
```

## Success Criteria

Both test scripts should show:
- ✅ All tests passed
- ✅ No connection errors
- ✅ Format can be set and retrieved
- ✅ Queries return data
- ✅ Format switching works
- ✅ No failures

## Performance Testing

To compare performance between Arrow IPC and PostgreSQL formats:

```bash
# Using test script (shows comparison)
./test_arrow_ipc.sh --comprehensive

# Using Python client (detailed timing)
python examples/arrow_ipc_client.py
```

## Integration with CI/CD

These scripts can be integrated into CI/CD pipelines:

```bash
#!/bin/bash
# Start server in background
./rust/cubesql/target/release/cubesqld &
SERVER_PID=$!

# Wait for startup
sleep 2

# Run tests
./test_arrow_ipc.sh --quick
TEST_RESULT=$?

# Cleanup
kill $SERVER_PID

# Exit with test result
exit $TEST_RESULT
```

## Notes

- **psql Required**: Both scripts require psql (PostgreSQL client) for testing
- **Network**: Tests assume CubeSQL is on localhost (127.0.0.1) by default
- **User**: Default user is 'root' (configurable with --user flag)
- **No Backend**: System table queries work without Cube.js backend
- **Sequential**: Tests run sequentially for reliability

## Additional Testing

For comprehensive Arrow IPC testing with actual data deserialization:

1. **Python**: See `examples/arrow_ipc_client.py`
   - Tests pandas integration
   - Tests Parquet export
   - Includes performance comparison

2. **JavaScript**: See `examples/arrow_ipc_client.js`
   - Tests Apache Arrow deserialization
   - Tests streaming
   - JSON export examples

3. **R**: See `examples/arrow_ipc_client.R`
   - Tests tidyverse integration
   - Tests data analysis workflows
   - Parquet export

## Command Reference

### test_arrow_ipc.sh
```bash
./test_arrow_ipc.sh                           # Full test suite
./test_arrow_ipc.sh --quick                   # Quick tests
./test_arrow_ipc.sh --host 192.168.1.10      # Custom host
./test_arrow_ipc.sh --port 5432              # Custom port
./test_arrow_ipc.sh --user postgres          # Custom user
./test_arrow_ipc.sh --help                   # Show help
```

### test_arrow_ipc_curl.sh
```bash
./test_arrow_ipc_curl.sh                      # Full test suite
./test_arrow_ipc_curl.sh --quick              # Quick tests
./test_arrow_ipc_curl.sh --host 192.168.1.10 # Custom host
./test_arrow_ipc_curl.sh --port 5432         # Custom port
./test_arrow_ipc_curl.sh --docs              # Show documentation
./test_arrow_ipc_curl.sh --help              # Show help
```

## Support

For issues or questions:
1. Check CubeSQL server logs: `CUBESQL_LOG_LEVEL=debug`
2. Verify server is running: `lsof -i :4444`
3. Test basic psql connection: `psql -h 127.0.0.1 -p 4444 -U root -c "SELECT 1"`
4. Check script requirements: `which psql`, `which nc`

---

**Script Location**: `/home/io/projects/learn_erl/cube/`
**Status**: Ready for production testing
**Last Updated**: December 1, 2025
