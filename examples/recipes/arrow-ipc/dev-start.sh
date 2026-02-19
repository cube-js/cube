#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo -e "${BLUE}======================================${NC}"
echo -e "${BLUE}Cube ADBC(Arrow Native) Development Setup${NC}"
echo -e "${BLUE}======================================${NC}"
echo ""

# Check if .env exists
if [ ! -f ".env" ]; then
    echo -e "${RED}Error: .env file not found${NC}"
    echo "Please create .env file based on .env.example"
    exit 1
fi

# Source environment
source .env

# Function to check if a port is in use
check_port() {
    local port=$1
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1 ; then
        return 0  # Port is in use
    else
        return 1  # Port is free
    fi
}

# Step 1: Start PostgreSQL
echo -e "${GREEN}Step 1: Starting PostgreSQL database...${NC}"
if check_port 7432; then
    echo -e "${YELLOW}PostgreSQL already running on port 7432${NC}"
else
    docker-compose up -d postgres
    echo "Waiting for PostgreSQL to be ready..."
    sleep 3
fi

# Step 2: Build cubesql with ADBC(Arrow Native) support
echo ""
echo -e "${GREEN}Step 2: Building cubesqld with ADBC(Arrow Native) support...${NC}"
CUBE_ROOT="$SCRIPT_DIR/../../.."
cd "$CUBE_ROOT/rust/cubesql"
cargo build --release --bin cubesqld
mkdir -p "$SCRIPT_DIR/bin"
cp "target/release/cubesqld" "$SCRIPT_DIR/bin/"
chmod +x "$SCRIPT_DIR/bin/cubesqld"
cd "$SCRIPT_DIR"

# Step 3: Start Cube.js API server
echo ""
echo -e "${GREEN}Step 3: Starting Cube.js API server...${NC}"
if check_port ${PORT:-4008}; then
    echo -e "${YELLOW}Cube.js API already running on port ${PORT:-4008}${NC}"
    CUBE_API_URL="http://localhost:${PORT:-4008}"
else
    echo "Starting Cube.js server in background..."
    yarn dev > cube-api.log 2>&1 &
    CUBE_API_PID=$!
    echo $CUBE_API_PID > cube-api.pid

    # Wait for Cube.js to be ready
    echo "Waiting for Cube.js API to be ready..."
    for i in {1..30}; do
        if check_port ${PORT:-4008}; then
            echo -e "${GREEN}Cube.js API is ready!${NC}"
            break
        fi
        sleep 1
    done

    CUBE_API_URL="http://localhost:${PORT:-4008}"
fi

# Generate a test token (in production this would be from auth)
# For dev mode, Cube.js typically uses 'test' or generates one
CUBE_TOKEN="${CUBESQL_CUBE_TOKEN:-test}"

# Step 4: Start cubesql with both PostgreSQL and ADBC(Arrow Native) protocols
echo ""
echo -e "${GREEN}Step 4: Starting cubesqld with ADBC(Arrow Native) support...${NC}"
echo ""
echo -e "${BLUE}Configuration:${NC}"
echo -e "  Cube.js API: ${CUBE_API_URL}/cubejs-api/v1"
echo -e "  PostgreSQL Port: ${CUBEJS_PG_SQL_PORT:-4444}"
echo -e "  ADBC Port: ${CUBEJS_ADBC_PORT:-8120}"
echo -e "  Log Level: ${CUBESQL_LOG_LEVEL:-info}"
echo ""
echo -e "${YELLOW}To test the connections:${NC}"
echo -e "  PostgreSQL: psql -h 127.0.0.1 -p ${CUBEJS_PG_SQL_PORT:-4444} -U root"
echo -e "  ADBC: Use ADBC driver on port ${CUBEJS_ADBC_PORT:-8120}"
echo ""
echo -e "${YELLOW}Logs:${NC}"
echo -e "  Cube.js API: tail -f $SCRIPT_DIR/cube-api.log"
echo -e "  cubesqld: See output below"
echo ""
echo -e "${YELLOW}Press Ctrl+C to stop${NC}"
echo ""

# Export environment variables for cubesqld
export CUBESQL_CUBE_URL="${CUBE_API_URL}/cubejs-api/v1"
export CUBESQL_CUBE_TOKEN="${CUBE_TOKEN}"
export CUBESQL_PG_PORT="${CUBEJS_PG_SQL_PORT:-4444}"
export CUBESQL_LOG_LEVEL="${CUBESQL_LOG_LEVEL:-info}"

# Cleanup function
cleanup() {
    echo ""
    echo -e "${YELLOW}Shutting down...${NC}"

    # Kill cubesql (handled by trap)

    # Optionally stop Cube.js API
    if [ -f cube-api.pid ]; then
        CUBE_PID=$(cat cube-api.pid)
        if ps -p $CUBE_PID > /dev/null 2>&1; then
            echo "Stopping Cube.js API (PID: $CUBE_PID)..."
            kill $CUBE_PID 2>/dev/null || true
            rm cube-api.pid
        fi
    fi

    echo -e "${GREEN}Cleanup complete${NC}"
}

trap cleanup EXIT

# Run cubesqld
exec ./bin/cubesqld
