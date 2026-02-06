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
echo -e "${BLUE}Quick Pre-Commit Checks${NC}"
echo -e "${BLUE}(Runs in ~1-2 minutes)${NC}"
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

# ============================================
# QUICK CHECKS (most likely to catch issues)
# ============================================

echo -e "${YELLOW}=== FORMAT CHECKS ===${NC}"
echo ""

run_test "Check Rust formatting" \
    "cd $CUBE_ROOT/rust/cubesql && cargo fmt --all -- --check && \
     cd $CUBE_ROOT/packages/cubejs-backend-native && cargo fmt --all -- --check && \
     cd $CUBE_ROOT/rust/cubenativeutils && cargo fmt --all -- --check && \
     cd $CUBE_ROOT/rust/cubesqlplanner && cargo fmt --all -- --check"

echo -e "${YELLOW}=== CLIPPY (CubeSQL only) ===${NC}"
echo ""

run_test "Clippy CubeSQL" \
    "cd $CUBE_ROOT/rust/cubesql && cargo clippy --workspace --all-targets -- -D warnings"

echo -e "${YELLOW}=== UNIT TESTS (CubeSQL only) ===${NC}"
echo ""

# Check if cargo-insta is installed
if ! command -v cargo-insta &> /dev/null; then
    echo -e "${YELLOW}Installing cargo-insta...${NC}"
    cargo install cargo-insta --version 1.42.0
fi

run_test "CubeSQL unit tests" \
    "cd $CUBE_ROOT/rust/cubesql && cargo insta test --all-features --unreferenced warn"

# ============================================
# SUMMARY
# ============================================

echo ""
echo -e "${BLUE}========================================${NC}"

if [ $FAILURES -eq 0 ]; then
    echo -e "${GREEN}✓ Quick checks passed!${NC}"
    echo ""
    echo -e "${YELLOW}Note: This is a quick check. Run ./run-ci-tests-local.sh for full CI tests.${NC}"
    exit 0
else
    echo -e "${RED}✗ $FAILURES check(s) failed${NC}"
    echo ""
    echo "Please fix the issues before committing."
    exit 1
fi
