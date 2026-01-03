#!/bin/bash
# Start only the Rust cubesqld server with ADBC Server and PostgreSQL protocols
# Requires Cube.js API server to be running (see start-cube-api.sh)

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}======================================${NC}"
echo -e "${BLUE}Cube SQL (cubesqld) Server${NC}"
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

# Check if Cube.js API is running
CUBE_API_PORT=${PORT:-4008}
echo -e "${GREEN}Checking Cube.js API server...${NC}"
if ! check_port ${CUBE_API_PORT}; then
    echo -e "${RED}Error: Cube.js API is NOT running on port ${CUBE_API_PORT}${NC}"
    echo ""
    echo "Please start it first with:"
    echo "  cd $SCRIPT_DIR"
    echo "  ./start-cube-api.sh"
    exit 1
fi
echo -e "${YELLOW}Cube.js API is running on port ${CUBE_API_PORT}${NC}"

# Check if cubesqld ports are free
#PG_PORT=${CUBEJS_PG_SQL_PORT:-4444}
ADBC_PORT=${CUBEJS_ADBC_PORT:-8120}

echo ""
echo -e "${GREEN}Checking port availability...${NC}"
if check_port ${PG_PORT}; then
    echo -e "${RED}Error: Port ${PG_PORT} is already in use${NC}"
    echo "Kill the process with: kill \$(lsof -ti:${PG_PORT})"
    exit 1
fi

if check_port ${ADBC_PORT}; then
    echo -e "${RED}Error: Port ${ADBC_PORT} is already in use${NC}"
    echo "Kill the process with: kill \$(lsof -ti:${ADBC_PORT})"
    exit 1
fi
echo -e "${YELLOW}Ports ${PG_PORT} and ${ADBC_PORT} are available${NC}"

# Determine cubesqld binary location
CUBE_ROOT="$SCRIPT_DIR/../../.."
CUBESQLD_DEBUG="$CUBE_ROOT/rust/cubesql/target/debug/cubesqld"
CUBESQLD_RELEASE="$CUBE_ROOT/rust/cubesql/target/release/cubesqld"
CUBESQLD_LOCAL="$SCRIPT_DIR/bin/cubesqld"

echo "---> "${CUBESQLD_RELEASE}

CUBESQLD_BIN=""
if [ -f "$CUBESQLD_DEBUG" ]; then
    CUBESQLD_BIN="$CUBESQLD_DEBUG"
    BUILD_TYPE="debug"
elif [ -f "$CUBESQLD_RELEASE" ]; then
    CUBESQLD_BIN="$CUBESQLD_RELEASE"
    BUILD_TYPE="release"
elif [ -f "$CUBESQLD_LOCAL" ]; then
    CUBESQLD_BIN="$CUBESQLD_LOCAL"
    BUILD_TYPE="local"
else
    echo -e "${RED}Error: cubesqld binary not found${NC}"
    echo ""
    echo "Build it with:"
    echo "  cd $CUBE_ROOT/rust/cubesql"
    echo "  cargo build --bin cubesqld          # for debug build"
    echo "  cargo build --release --bin cubesqld # for release build"
    exit 1
fi

echo ""
echo -e "${GREEN}Found cubesqld binary (${BUILD_TYPE}):${NC}"
echo "  $CUBESQLD_BIN"

# Set environment variables for cubesqld
CUBE_API_URL="http://localhost:${CUBE_API_PORT}/cubejs-api"
CUBE_TOKEN="${CUBESQL_CUBE_TOKEN:-test}"

export CUBESQL_CUBE_URL="${CUBE_API_URL}"
export CUBESQL_CUBE_TOKEN="${CUBE_TOKEN}"
export CUBEJS_ADBC_PORT="${ADBC_PORT}"
export CUBESQL_LOG_LEVEL="${CUBESQL_LOG_LEVEL:-error}"
export CUBESTORE_LOG_LEVEL="error"

# Enable Arrow Results Cache (default: true, can be overridden)
export CUBESQL_ARROW_RESULTS_CACHE_ENABLED="${CUBESQL_ARROW_RESULTS_CACHE_ENABLED:-true}"
export CUBESQL_ARROW_RESULTS_CACHE_MAX_ENTRIES="${CUBESQL_ARROW_RESULTS_CACHE_MAX_ENTRIES:-1000}"
export CUBESQL_ARROW_RESULTS_CACHE_TTL="${CUBESQL_ARROW_RESULTS_CACHE_TTL:-3600}"

echo ""
echo -e "${BLUE}Configuration:${NC}"
echo -e "  Cube API URL: ${CUBESQL_CUBE_URL}"
echo -e "  Cube Token: ${CUBESQL_CUBE_TOKEN}"
echo -e "  PostgreSQL Port: ${CUBESQL_PG_PORT}"
echo -e "  ADBC Port: ${CUBEJS_ADBC_PORT}"
echo -e "  Log Level: ${CUBESQL_LOG_LEVEL}"
echo -e "  Arrow Results Cache: ${CUBESQL_ARROW_RESULTS_CACHE_ENABLED} (max: ${CUBESQL_ARROW_RESULTS_CACHE_MAX_ENTRIES}, ttl: ${CUBESQL_ARROW_RESULTS_CACHE_TTL}s)"
echo ""
echo ""
echo -e "${YELLOW}Press Ctrl+C to stop${NC}"
echo ""

# Cleanup function
cleanup() {
    echo ""
    echo -e "${YELLOW}Shutting down cubesqld...${NC}"
    echo -e "${GREEN}Cleanup complete${NC}"
}

trap cleanup EXIT

# Run cubesqld
exec "$CUBESQLD_BIN"
