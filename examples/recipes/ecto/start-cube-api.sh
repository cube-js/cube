#!/bin/bash
# Start only the Cube.js API server (without Arrow/PostgreSQL protocols)
# This allows cubesqld to handle the protocols instead

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
echo -e "${BLUE}Cube.js API Server (Standalone)${NC}"
echo -e "${BLUE}======================================${NC}"
echo ""

# Check if .env exists
if [ ! -f ".env" ]; then
    echo -e "${RED}Error: .env file not found${NC}"
    echo "Please create .env file based on .env.example"
    exit 1
fi

# Source environment - but override protocol ports to disable them
source .env

# Override to disable built-in protocol servers
# (cubesqld will provide these instead)
unset CUBEJS_PG_SQL_PORT
export CUBEJS_PG_SQL_PORT="9432"
#export CUBEJS_ADBC_PORT="8120"
#export CUBEJS_SQL_PORT="4445"

export PORT=${PORT:-4008}

export CUBEJS_DB_TYPE=${CUBEJS_DB_TYPE:-postgres}
export CUBEJS_DB_PORT=${CUBEJS_DB_PORT:-8432}
export CUBEJS_DB_NAME=${CUBEJS_DB_NAME:-pot_examples_dev}
export CUBEJS_DB_USER=${CUBEJS_DB_USER:-postgres}
export CUBEJS_DB_PASS=${CUBEJS_DB_PASS:-postgres}
export CUBEJS_DB_HOST=${CUBEJS_DB_HOST:-localhost}
export CUBEJS_DEV_MODE=${CUBEJS_DEV_MODE:-true}
export CUBEJS_LOG_LEVEL=${CUBEJS_LOG_LEVEL:-trace}
export CUBESTORE_LOG_LEVEL=${CUBEJS_LOG_LEVEL:-trace}
export NODE_ENV=${NODE_ENV:-development}

# Function to check if a port is in use
check_port() {
    local port=$1
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1 ; then
        return 0  # Port is in use
    else
        return 1  # Port is free
    fi
}

# Check PostgreSQL
echo -e "${GREEN}Checking PostgreSQL database...${NC}"
if check_port ${CUBEJS_DB_PORT}; then
    echo -e "${YELLOW}PostgreSQL is running on port ${CUBEJS_DB_PORT}${NC}"
else
    echo -e "${YELLOW}PostgreSQL is NOT running on port ${CUBEJS_DB_PORT}${NC}"
    echo "Starting PostgreSQL with docker-compose..."
    docker-compose up -d postgres
    sleep 3
fi

# Check if API is already running
echo ""
echo -e "${GREEN}Starting Cube.js API server...${NC}"
if check_port ${PORT}; then
    echo -e "${YELLOW}Cube.js API already running on port ${PORT}${NC}"
    echo "Kill it first with: kill \$(lsof -ti:${PORT})"
    exit 1
fi

echo ""
echo -e "${BLUE}Configuration:${NC}"
echo -e "  API Port: ${PORT}"
echo -e "  API URL: http://localhost:${PORT}/cubejs-api"
echo -e "  Database: ${CUBEJS_DB_TYPE} at ${CUBEJS_DB_HOST}:${CUBEJS_DB_PORT}"
echo -e "  Database Name: ${CUBEJS_DB_NAME}"
echo -e "  Log Level: ${CUBEJS_LOG_LEVEL}"
echo ""
echo -e "${YELLOW}Note: PostgreSQL and ADBC(Arrow Native) protocols are DISABLED${NC}"
echo -e "${YELLOW}      Use cubesqld for those (see start-cubesqld.sh)${NC}"
echo ""
echo -e "${YELLOW}Logs will be written to: $SCRIPT_DIR/cube-api.log${NC}"
echo -e "${YELLOW}Press Ctrl+C to stop${NC}"
echo ""

# Cleanup function
cleanup() {
    echo ""
    echo -e "${YELLOW}Shutting down Cube.js API...${NC}"
    echo -e "${GREEN}Cleanup complete${NC}"
}

trap cleanup EXIT

# Run Cube.js API server
env | grep CUBE | sort
exec yarn dev 2>&1 | tee cube-api.log
