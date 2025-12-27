#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Building Cube with ADBC(Arrow Native) Support${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Get the root directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CUBE_ROOT="$SCRIPT_DIR/../../.."
CUBESQL_DIR="$CUBE_ROOT/rust/cubesql"

# Build cubesql binary
echo -e "${GREEN}Step 1: Building cubesqld binary...${NC}"
cd "$CUBESQL_DIR"
cargo build --release --bin cubesqld

# Copy binary to dev project bin directory
echo -e "${GREEN}Step 2: Copying cubesqld binary...${NC}"
mkdir -p "$SCRIPT_DIR/bin"
cp "$CUBESQL_DIR/target/release/cubesqld" "$SCRIPT_DIR/bin/"
chmod +x "$SCRIPT_DIR/bin/cubesqld"

echo ""
echo -e "${GREEN}Build complete!${NC}"
echo ""
echo -e "${YELLOW}Binary location: $SCRIPT_DIR/bin/cubesqld${NC}"
echo ""

# Check if .env file exists
if [ ! -f "$SCRIPT_DIR/.env" ]; then
    echo -e "${YELLOW}Warning: .env file not found. Please create one based on .env.example${NC}"
    exit 1
fi

# Source the .env file to get configuration
source "$SCRIPT_DIR/.env"

# Start the server
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Starting Cube SQL Server${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "${GREEN}Configuration:${NC}"
echo -e "  PostgreSQL Port: ${CUBEJS_PG_SQL_PORT:-4444}"
echo -e "  ADBC Port: ${CUBEJS_ADBC_PORT:-8120}"
echo -e "  Database: ${CUBEJS_DB_TYPE}://${CUBEJS_DB_USER}@${CUBEJS_DB_HOST}:${CUBEJS_DB_PORT}/${CUBEJS_DB_NAME}"
echo -e "  Log Level: ${CUBESQL_LOG_LEVEL:-info}"
echo ""
echo -e "${YELLOW}Press Ctrl+C to stop the server${NC}"
echo ""

# Export environment variables for cubesqld
export CUBESQL_PG_PORT="${CUBEJS_PG_SQL_PORT:-4444}"
export CUBESQL_LOG_LEVEL="${CUBESQL_LOG_LEVEL:-info}"

# Run the cubesqld binary
cd "$SCRIPT_DIR"
exec "./bin/cubesqld"
