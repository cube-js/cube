#!/bin/bash

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "Verifying Cube ADBC(Arrow Native) Build"
echo "=================================="
echo ""

# Check if binary exists
if [ ! -f "bin/cubesqld" ]; then
    echo -e "${RED}✗ cubesqld binary not found${NC}"
    echo "Run: ./dev-start.sh to build"
    exit 1
fi

echo -e "${GREEN}✓ cubesqld binary found ($(ls -lh bin/cubesqld | awk '{print $5}'))${NC}"

# Check for ADBC(Arrow Native) symbols
if nm bin/cubesqld 2>/dev/null | grep -q "ArrowNativeServer"; then
    echo -e "${GREEN}✓ ArrowNativeServer symbol found in binary${NC}"
else
    echo -e "${YELLOW}⚠ Cannot verify ArrowNativeServer symbol (may be optimized)${NC}"
fi

# Test environment variable parsing
echo ""
echo "Testing configuration parsing..."
export CUBEJS_ADBC_PORT=8120
export CUBESQL_PG_PORT=4444
export CUBESQL_LOG_LEVEL=error

# Start cubesql in background and check output
timeout 3 bin/cubesqld 2>&1 | head -20 &
CUBESQL_PID=$!
sleep 2

# Check if it's listening on the Arrow port
if lsof -Pi :8120 -sTCP:LISTEN -t >/dev/null 2>&1 ; then
    echo -e "${GREEN}✓ ADBC(Arrow Native) server listening on port 8120${NC}"
    ARROW_OK=1
else
    echo -e "${RED}✗ ADBC(Arrow Native) server NOT listening on port 8120${NC}"
    ARROW_OK=0
fi

# Check PostgreSQL port
if lsof -Pi :4444 -sTCP:LISTEN -t >/dev/null 2>&1 ; then
    echo -e "${GREEN}✓ PostgreSQL server listening on port 4444${NC}"
    PG_OK=1
else
    echo -e "${RED}✗ PostgreSQL server NOT listening on port 4444${NC}"
    PG_OK=0
fi

# Cleanup
kill $CUBESQL_PID 2>/dev/null || true
sleep 1

echo ""
echo "Summary"
echo "======="

if [ $ARROW_OK -eq 1 ] && [ $PG_OK -eq 1 ]; then
    echo -e "${GREEN}✓ Both protocols are working correctly!${NC}"
    echo ""
    echo "You can now:"
    echo "  - Connect via PostgreSQL: psql -h 127.0.0.1 -p 4444 -U root"
    echo "  - Connect via ADBC: Use ADBC driver on port 8120"
    echo ""
    echo "To start the full dev environment:"
    echo "  ./dev-start.sh"
    exit 0
else
    echo -e "${RED}✗ Some protocols failed to start${NC}"
    echo ""
    echo "This may be because:"
    echo "  - Cube.js API is not running (needed for query execution)"
    echo "  - Ports are already in use"
    echo ""
    echo "Try running the full stack:"
    echo "  ./dev-start.sh"
    exit 1
fi
