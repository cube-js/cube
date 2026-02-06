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

# Ask about deep clean
echo -e "${YELLOW}Do you want to perform a deep clean first?${NC}"
echo "This will remove all caches, build artifacts, and node_modules."
echo "Choose this after major rebases or when experiencing build issues."
echo ""
echo "Options:"
echo "  1) Quick rebuild (incremental, fastest)"
echo "  2) Deep clean + full rebuild (removes everything, slowest but safest)"
echo ""
read -p "Choose option (1/2) [default: 1]: " -n 1 -r
echo ""
echo ""

DEEP_CLEAN=false
if [[ $REPLY == "2" ]]; then
    DEEP_CLEAN=true
    echo -e "${RED}⚠️  DEEP CLEAN MODE ENABLED${NC}"
    echo "This will remove:"
    echo "  - All node_modules directories"
    echo "  - All Rust target directories"
    echo "  - All TypeScript build artifacts"
    echo "  - Recipe binaries and caches"
    echo ""
    read -p "Are you sure? This will take 5-10 minutes to rebuild. (y/n): " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Cancelled. Running quick rebuild instead..."
        DEEP_CLEAN=false
    fi
    echo ""
fi

# Function to check if a command succeeded
check_status() {
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ $1${NC}"
    else
        echo -e "${RED}✗ $1 failed${NC}"
        exit 1
    fi
}

# Deep clean if requested
if [ "$DEEP_CLEAN" = true ]; then
    echo -e "${BLUE}======================================${NC}"
    echo -e "${BLUE}Deep Clean Phase${NC}"
    echo -e "${BLUE}======================================${NC}"
    echo ""

    # Clean recipe directory
    echo -e "${GREEN}Cleaning recipe directory...${NC}"
    cd "$SCRIPT_DIR"
    rm -rf node_modules yarn.lock bin .cubestore *.log *.pid
    check_status "Recipe directory cleaned"

    # Clean Cube.js build artifacts
    echo ""
    echo -e "${GREEN}Cleaning Cube.js build artifacts...${NC}"
    cd "$CUBE_ROOT"

    # Use yarn clean if available
    if grep -q '"clean"' package.json; then
        yarn clean
        check_status "Cube.js build artifacts cleaned"
    else
        echo -e "${YELLOW}No clean script found, manually cleaning dist directories${NC}"
        find packages -type d -name "dist" -exec rm -rf {} + 2>/dev/null || true
        find packages -type d -name "lib" -exec rm -rf {} + 2>/dev/null || true
        find packages -type f -name "tsconfig.tsbuildinfo" -delete 2>/dev/null || true
        check_status "Manual cleanup complete"
    fi

    # Clean node_modules (this is the slowest part)
    echo ""
    echo -e "${GREEN}Removing node_modules...${NC}"
    echo -e "${YELLOW}This may take 1-2 minutes...${NC}"
    cd "$CUBE_ROOT"
    rm -rf node_modules
    check_status "node_modules removed"

    # Clean Rust target directories
    echo ""
    echo -e "${GREEN}Cleaning Rust build artifacts...${NC}"
    cd "$CUBE_ROOT/rust/cubesql"
    if [ -d "target" ]; then
        rm -rf target
        check_status "CubeSQL target directory removed"
    else
        echo -e "${YELLOW}CubeSQL target directory not found, skipping${NC}"
    fi

    # Clean other Rust crates if they exist
    for rust_dir in "$CUBE_ROOT/rust"/*; do
        if [ -d "$rust_dir/target" ]; then
            echo -e "${YELLOW}Cleaning $(basename $rust_dir)/target${NC}"
            rm -rf "$rust_dir/target"
        fi
    done

    if [ -d "$CUBE_ROOT/packages/cubejs-backend-native/target" ]; then
        echo -e "${YELLOW}Cleaning cubejs-backend-native/target${NC}"
        rm -rf "$CUBE_ROOT/packages/cubejs-backend-native/target"
    fi

    check_status "All Rust artifacts cleaned"

    echo ""
    echo -e "${GREEN}✓ Deep clean complete!${NC}"
    echo ""
    echo -e "${BLUE}======================================${NC}"
    echo -e "${BLUE}Rebuild Phase${NC}"
    echo -e "${BLUE}======================================${NC}"
    echo ""
fi

# Step 1: Install root dependencies (skip post-install scripts first)
echo -e "${GREEN}Step 1: Installing root dependencies...${NC}"
cd "$CUBE_ROOT"

# If deep clean was done, need to install without post-install scripts first
# because post-install scripts depend on built packages
if [ "$DEEP_CLEAN" = true ]; then
    echo -e "${YELLOW}Installing without post-install scripts (packages not built yet)...${NC}"
    yarn install --ignore-scripts
    check_status "Dependencies installed (scripts skipped)"
else
    yarn install
    check_status "Root dependencies installed"
fi

# Step 2: Build all packages (TypeScript + client bundles)
echo ""
echo -e "${GREEN}Step 2: Building TypeScript packages...${NC}"
echo -e "${YELLOW}This may take 30-40 seconds...${NC}"
cd "$CUBE_ROOT"

# Use yarn tsc which runs "tsc --build" for proper TypeScript project references
yarn tsc
check_status "TypeScript packages built"

echo ""
echo -e "${GREEN}Step 2b: Building client bundles...${NC}"
cd "$CUBE_ROOT"
yarn build
check_status "Client bundles built"

# Step 2c: Generate oclif manifest for cubejs-server
echo ""
echo -e "${GREEN}Step 2c: Generating oclif manifest...${NC}"
cd "$CUBE_ROOT/packages/cubejs-server"
OCLIF_TS_NODE=0 yarn run oclif-dev manifest
check_status "Oclif manifest generated"
cd "$CUBE_ROOT"

# Step 2.5: Re-run install with post-install scripts if they were skipped
if [ "$DEEP_CLEAN" = true ]; then
    echo ""
    echo -e "${GREEN}Step 2.5: Running post-install scripts...${NC}"
    echo -e "${YELLOW}(Optional module failures can be safely ignored)${NC}"
    cd "$CUBE_ROOT"
    # Allow post-install to fail on optional modules
    yarn install || true
    echo -e "${GREEN}✓ Install completed (some optional modules may have failed)${NC}"
fi

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

# Step 4: Build CubeSQL (optional - ask user, or automatic after deep clean)
echo ""
echo -e "${YELLOW}Step 4: Build CubeSQL?${NC}"

# Automatic build after deep clean (since we removed target directory)
BUILD_CUBESQL=false
if [ "$DEEP_CLEAN" = true ]; then
    echo -e "${YELLOW}Deep clean was performed, CubeSQL must be rebuilt.${NC}"
    BUILD_CUBESQL=true
else
    echo "Building CubeSQL (Rust) takes 5-10 minutes."
    read -p "Build CubeSQL now? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        BUILD_CUBESQL=true
    fi
fi

if [ "$BUILD_CUBESQL" = true ]; then
    echo -e "${GREEN}Building CubeSQL...${NC}"
    cd "$CUBE_ROOT/rust/cubesql"

    # Check if we should do release or debug build
    if [ "$DEEP_CLEAN" = true ]; then
        # Default to release build after deep clean
        echo -e "${YELLOW}Deep clean mode: building release version (recommended)${NC}"
        echo "This will take 5-10 minutes..."
        cargo build --release --bin cubesqld
        check_status "CubeSQL built (release)"
        CUBESQLD_BIN="$CUBE_ROOT/rust/cubesql/target/release/cubesqld"
    else
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

# Show what was done
if [ "$DEEP_CLEAN" = true ]; then
    echo -e "${GREEN}✓ Deep clean performed${NC}"
    echo "  - Removed all caches and build artifacts"
    echo "  - Fresh install of all dependencies"
    echo "  - Complete rebuild of all packages"
    echo ""
fi

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
