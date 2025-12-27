#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CUBE_ROOT="$SCRIPT_DIR/../../.."

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Running Tests Only${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

FAILURES=0

run_test() {
    local name="$1"
    local command="$2"

    echo -e "${BLUE}>>> $name${NC}"
    if eval "$command"; then
        echo -e "${GREEN}✓ $name passed${NC}"
        echo ""
        return 0
    else
        echo -e "${RED}✗ $name failed${NC}"
        echo ""
        FAILURES=$((FAILURES + 1))
        return 1
    fi
}

# Check if cargo-insta is installed
if ! command -v cargo-insta &> /dev/null; then
    echo -e "${YELLOW}Installing cargo-insta...${NC}"
    cargo install cargo-insta --version 1.42.0
    echo ""
fi

# ============================================
# RUST UNIT TESTS
# ============================================

echo -e "${YELLOW}=== RUST UNIT TESTS ===${NC}"
echo ""

run_test "CubeSQL unit tests (Rewrite Engine)" \
    "cd $CUBE_ROOT/rust/cubesql && \
     export CUBESQL_SQL_PUSH_DOWN=true && \
     export CUBESQL_REWRITE_CACHE=true && \
     export CUBESQL_REWRITE_TIMEOUT=60 && \
     cargo insta test --all-features --workspace --unreferenced warn"

# ============================================
# NATIVE TESTS (if built)
# ============================================

if [ -f "$CUBE_ROOT/packages/cubejs-backend-native/index.node" ]; then
    echo -e "${YELLOW}=== NATIVE TESTS ===${NC}"
    echo ""

    run_test "Native unit tests" \
        "cd $CUBE_ROOT/packages/cubejs-backend-native && \
         export CUBESQL_STREAM_MODE=true && \
         export CUBEJS_NATIVE_INTERNAL_DEBUG=true && \
         yarn run test:unit"
else
    echo -e "${YELLOW}Skipping native tests (not built)${NC}"
    echo -e "${YELLOW}Run: cd packages/cubejs-backend-native && yarn run native:build-debug${NC}"
    echo ""
fi

# ============================================
# SUMMARY
# ============================================

echo ""
echo -e "${BLUE}========================================${NC}"

if [ $FAILURES -eq 0 ]; then
    echo -e "${GREEN}✓ All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}✗ $FAILURES test(s) failed${NC}"
    exit 1
fi
