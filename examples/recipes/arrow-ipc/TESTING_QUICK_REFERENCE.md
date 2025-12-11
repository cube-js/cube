# Arrow IPC Testing - Quick Reference Card

## 🚀 Start Testing (Copy & Paste)

### Terminal 1: Start Server
```bash
cd /home/io/projects/learn_erl/cube
CUBESQL_LOG_LEVEL=debug ./rust/cubesql/target/release/cubesqld
```

### Terminal 2: Run Tests
```bash
cd /home/io/projects/learn_erl/cube

# Option A: Full test suite
./test_arrow_ipc.sh

# Option B: Quick test (faster)
./test_arrow_ipc.sh --quick

# Option C: Protocol-level test
./test_arrow_ipc_curl.sh

# Option D: Manual psql testing
psql -h 127.0.0.1 -p 4444 -U root
```

## 📋 Manual Testing with psql

```bash
# Connect
psql -h 127.0.0.1 -p 4444 -U root

# Check default format
SELECT version();
SHOW output_format;

# Enable Arrow IPC
SET output_format = 'arrow_ipc';

# Verify it's set
SHOW output_format;

# Test query
SELECT * FROM information_schema.tables LIMIT 5;

# Switch back to PostgreSQL
SET output_format = 'postgresql';

# Exit
\q
```

## 🧪 Python Client Testing

```bash
# Install dependencies
pip install psycopg2-binary pyarrow pandas

# Run example
cd /home/io/projects/learn_erl/cube
python examples/arrow_ipc_client.py
```

## 🌐 JavaScript Client Testing

```bash
# Install dependencies
npm install pg apache-arrow

# Run example
cd /home/io/projects/learn_erl/cube
node examples/arrow_ipc_client.js
```

## 📊 R Client Testing

```bash
# Install dependencies in R
install.packages(c("RPostgres", "arrow", "tidyverse", "dplyr", "R6"))

# Run example
cd /home/io/projects/learn_erl/cube
Rscript -e "source('examples/arrow_ipc_client.R'); run_all_examples()"
```

## ✅ Success Indicators

When everything works, you'll see:
```
✓ CubeSQL is running on 127.0.0.1:4444
✓ Connected to CubeSQL
✓ Default format is 'postgresql'
✓ SET output_format succeeded
✓ Output format is now 'arrow_ipc'
✓ Query with Arrow IPC returned data
✓ All tests passed!
```

## ❌ Common Issues & Fixes

| Issue | Fix |
|-------|-----|
| "Connection refused" | Start server: `./rust/cubesql/target/release/cubesqld` |
| "psql: command not found" | Install: `apt-get install postgresql-client` |
| "Port 4444 in use" | Kill existing: `lsof -i :4444 \| grep LISTEN \| awk '{print $2}' \| xargs kill` |
| "output_format not recognized" | Use quotes: `SET output_format = 'arrow_ipc'` |
| "No data returned" | Check query: `SELECT * FROM information_schema.tables` |

## 📁 Files Overview

```
Binary:
  ./rust/cubesql/target/release/cubesqld           Main server

Test Scripts:
  ./test_arrow_ipc.sh                             Full tests with psql
  ./test_arrow_ipc_curl.sh                        Protocol-level tests
  ./TEST_SCRIPTS_README.md                        Script documentation

Client Examples:
  ./examples/arrow_ipc_client.py                  Python (5 examples)
  ./examples/arrow_ipc_client.js                  JavaScript (5 examples)
  ./examples/arrow_ipc_client.R                   R (6 examples)

Documentation:
  ./QUICKSTART_ARROW_IPC.md                       5-minute start
  ./TESTING_ARROW_IPC.md                          Comprehensive guide
  ./FULL_BUILD_SUMMARY.md                         Build info
  ./examples/ARROW_IPC_GUIDE.md                   Feature documentation
  ./PHASE_3_SUMMARY.md                            Technical details
```

## 🎯 Test Paths by Time Available

### 5 Minutes
```bash
# Start server
./rust/cubesql/target/release/cubesqld &

# Quick test
./test_arrow_ipc.sh --quick
```

### 15 Minutes
```bash
# Start server
./rust/cubesql/target/release/cubesqld &

# Full test suite
./test_arrow_ipc.sh

# Or manual testing with psql
psql -h 127.0.0.1 -p 4444 -U root
```

### 30 Minutes
```bash
# Start server
./rust/cubesql/target/release/cubesqld &

# Run all test scripts
./test_arrow_ipc.sh
./test_arrow_ipc_curl.sh

# Test with Python
python examples/arrow_ipc_client.py
```

### 1+ Hour
```bash
# Do all of the above, plus:

# Test with JavaScript
npm install pg apache-arrow
node examples/arrow_ipc_client.js

# Test with R
Rscript -e "source('examples/arrow_ipc_client.R'); run_all_examples()"

# Read full documentation
# - QUICKSTART_ARROW_IPC.md
# - TESTING_ARROW_IPC.md
# - examples/ARROW_IPC_GUIDE.md
```

## 📊 Expected Test Results

```
Arrow IPC Unit Tests:          7/7 PASSED ✓
Portal Execution Tests:        6/6 PASSED ✓
Integration Tests:             7/7 READY ✓
Total Tests:                   690 PASSED ✓
Regressions:                   NONE ✓
```

## 🔍 Monitoring Server

```bash
# Watch server logs in real-time (Terminal 3)
tail -f /var/log/cubesql.log

# Or restart with debug output
CUBESQL_LOG_LEVEL=debug ./rust/cubesql/target/release/cubesqld

# Check port is listening
lsof -i :4444
netstat -tulpn | grep 4444
```

## 💡 Pro Tips

1. **Use `--quick` for fast tests**: `./test_arrow_ipc.sh --quick`
2. **Enable debug logging**: `CUBESQL_LOG_LEVEL=debug`
3. **Test system tables first**: No backend needed
4. **Watch logs while testing**: Open another terminal with `tail -f`
5. **Verify format switching**: It's the easiest way to prove feature works

## 🎬 Demo Commands (Copy & Paste to psql)

```sql
-- Show we're connected
SELECT version();

-- Check default format
SHOW output_format;

-- Enable Arrow IPC
SET output_format = 'arrow_ipc';

-- Confirm it's set
SHOW output_format;

-- Query system tables (no backend needed)
SELECT count(*) FROM information_schema.tables;

-- Get specific tables
SELECT table_name, table_type
FROM information_schema.tables
LIMIT 10;

-- Switch back
SET output_format = 'postgresql';

-- Verify switched
SHOW output_format;

-- One more test
SELECT * FROM information_schema.schemata;
```

## 📞 Documentation to Read

| Doc | Time | Content |
|-----|------|---------|
| QUICKSTART_ARROW_IPC.md | 5 min | Get started fast |
| TEST_SCRIPTS_README.md | 5 min | Script usage |
| TESTING_ARROW_IPC.md | 15 min | All testing options |
| examples/ARROW_IPC_GUIDE.md | 20 min | Feature details |
| PHASE_3_SUMMARY.md | 15 min | Technical info |
| FULL_BUILD_SUMMARY.md | 10 min | Build details |

---

## ✨ You're Ready to Test!

**Next Step**: Open Terminal 1 and run the server command above, then open Terminal 2 and run the tests.

**Need Help?** See `TEST_SCRIPTS_README.md` for detailed documentation.

---

**Status**: ✅ Ready for Testing
**Date**: December 1, 2025
**Build**: Release (Optimized)
