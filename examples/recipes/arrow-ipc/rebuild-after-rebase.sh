#!/bin/bash
# Rebuild Cube.js and CubeSQL after git rebase
# This script rebuilds all necessary components for the arrow-ipc recipe

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CUBE_ROOT="$SCRIPT_DIR/../../.."

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}======================================${NC}"
echo -e "${BLUE}Rebuild After Rebase${NC}"
echo -e "${BLUE}======================================${NC}"
echo ""
echo "This script will rebuild:"
echo "  1. Cube.js packages (TypeScript)"
echo "  2. CubeSQL binary (Rust)"
echo ""

# Function to check if a command succeeded
check_status() {
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ $1${NC}"
    else
        echo -e "${RED}✗ $1 failed${NC}"
        exit 1
    fi
}

# Step 1: Install root dependencies
echo -e "${GREEN}Step 1: Installing root dependencies...${NC}"
cd "$CUBE_ROOT"
yarn install
check_status "Root dependencies installed"

# Step 2: Build all packages (TypeScript + client bundles)
echo ""
echo -e "${GREEN}Step 2: Building all packages...${NC}"
echo -e "${YELLOW}This may take 1-2 minutes...${NC}"
cd "$CUBE_ROOT"
yarn build
check_status "All packages built"

# Step 3: Verify workspace setup
echo ""
echo -e "${GREEN}Step 3: Verifying workspace setup...${NC}"
cd "$SCRIPT_DIR"

# Remove local yarn.lock if it exists (should use root workspace)
if [ -f "yarn.lock" ]; then
    echo -e "${YELLOW}Removing local yarn.lock (using root workspace instead)${NC}"
    rm yarn.lock
fi

# Remove local node_modules if it exists (should use root workspace)
if [ -d "node_modules" ]; then
    echo -e "${YELLOW}Removing local node_modules (using root workspace instead)${NC}"
    rm -rf node_modules
fi

echo -e "${GREEN}✓ Recipe will use root workspace dependencies${NC}"

# Step 4: Build CubeSQL (optional - ask user)
echo ""
echo -e "${YELLOW}Step 4: Build CubeSQL?${NC}"
echo "Building CubeSQL (Rust) takes 5-10 minutes."
read -p "Build CubeSQL now? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${GREEN}Building CubeSQL...${NC}"
    cd "$CUBE_ROOT/rust/cubesql"

    # Check if we should do release or debug build
    echo -e "${YELLOW}Build type:${NC}"
    echo "  1) Debug (faster build, slower runtime)"
    echo "  2) Release (slower build, faster runtime)"
    read -p "Choose build type (1/2): " -n 1 -r
    echo

    if [[ $REPLY == "2" ]]; then
        cargo build --release --bin cubesqld
        check_status "CubeSQL built (release)"
        CUBESQLD_BIN="$CUBE_ROOT/rust/cubesql/target/release/cubesqld"
    else
        cargo build --bin cubesqld
        check_status "CubeSQL built (debug)"
        CUBESQLD_BIN="$CUBE_ROOT/rust/cubesql/target/debug/cubesqld"
    fi

    # Copy to recipe bin directory
    mkdir -p "$SCRIPT_DIR/bin"
    cp "$CUBESQLD_BIN" "$SCRIPT_DIR/bin/"
    chmod +x "$SCRIPT_DIR/bin/cubesqld"
    echo -e "${GREEN}✓ CubeSQL binary copied to recipe/bin/${NC}"
else
    echo -e "${YELLOW}Skipping CubeSQL build${NC}"
    echo "You can build it later with:"
    echo "  cd $CUBE_ROOT/rust/cubesql"
    echo "  cargo build --release --bin cubesqld"
fi

# Step 5: Verify the build
echo ""
echo -e "${GREEN}Step 5: Verifying build...${NC}"

# Check if cubejs-server-core dist exists
if [ -d "$CUBE_ROOT/packages/cubejs-server-core/dist" ]; then
    echo -e "${GREEN}✓ Cube.js server-core dist found${NC}"
else
    echo -e "${RED}✗ Cube.js server-core dist not found${NC}"
    exit 1
fi

# Check if cubesqld exists
if [ -f "$SCRIPT_DIR/bin/cubesqld" ]; then
    echo -e "${GREEN}✓ CubeSQL binary found in recipe/bin/${NC}"
elif [ -f "$CUBE_ROOT/rust/cubesql/target/release/cubesqld" ]; then
    echo -e "${YELLOW}⚠ CubeSQL binary found in target/release/ but not copied to recipe/bin/${NC}"
elif [ -f "$CUBE_ROOT/rust/cubesql/target/debug/cubesqld" ]; then
    echo -e "${YELLOW}⚠ CubeSQL binary found in target/debug/ but not copied to recipe/bin/${NC}"
else
    echo -e "${YELLOW}⚠ CubeSQL binary not found (you can build it later)${NC}"
fi

# Done!
echo ""
echo -e "${BLUE}======================================${NC}"
echo -e "${GREEN}Rebuild Complete!${NC}"
echo -e "${BLUE}======================================${NC}"
echo ""
echo "You can now start the services:"
echo ""
echo -e "${YELLOW}Start Cube.js API server:${NC}"
echo "  cd $SCRIPT_DIR"
echo "  ./start-cube-api.sh"
echo ""
echo -e "${YELLOW}Start CubeSQL server:${NC}"
echo "  cd $SCRIPT_DIR"
echo "  ./start-cubesqld.sh"
echo ""
echo -e "${YELLOW}Or start everything:${NC}"
echo "  cd $SCRIPT_DIR"
echo "  ./dev-start.sh"
echo ""
