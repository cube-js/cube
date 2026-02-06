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
echo -e "${BLUE}Running Local CI Tests (like GitHub)${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Track failures
FAILURES=0

# Function to run a test step
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
# 1. LINT CHECKS (fmt + clippy)
# ============================================

echo -e "${YELLOW}=== LINT CHECKS ===${NC}"
echo ""

run_test "Lint CubeSQL (fmt)" \
    "cd $CUBE_ROOT/rust/cubesql && cargo fmt --all -- --check"

run_test "Lint Native (fmt)" \
    "cd $CUBE_ROOT/packages/cubejs-backend-native && cargo fmt --all -- --check"

run_test "Lint cubenativeutils (fmt)" \
    "cd $CUBE_ROOT/rust/cubenativeutils && cargo fmt --all -- --check"

run_test "Lint cubesqlplanner (fmt)" \
    "cd $CUBE_ROOT/rust/cubesqlplanner && cargo fmt --all -- --check"

run_test "Clippy CubeSQL" \
    "cd $CUBE_ROOT/rust/cubesql && cargo clippy --locked --workspace --all-targets --keep-going -- -D warnings"

run_test "Clippy Native" \
    "cd $CUBE_ROOT/packages/cubejs-backend-native && cargo clippy --locked --workspace --all-targets --keep-going -- -D warnings"

run_test "Clippy cubenativeutils" \
    "cd $CUBE_ROOT/rust/cubenativeutils && cargo clippy --locked --workspace --all-targets --keep-going -- -D warnings"

run_test "Clippy cubesqlplanner" \
    "cd $CUBE_ROOT/rust/cubesqlplanner && cargo clippy --locked --workspace --all-targets --keep-going -- -D warnings"

# ============================================
# 2. UNIT TESTS (Rewrite Engine)
# ============================================

echo -e "${YELLOW}=== UNIT TESTS ===${NC}"
echo ""

# Check if cargo-insta is installed
if ! command -v cargo-insta &> /dev/null; then
    echo -e "${YELLOW}Installing cargo-insta...${NC}"
    cargo install cargo-insta --version 1.42.0
fi

run_test "Unit tests (Rewrite Engine)" \
    "cd $CUBE_ROOT/rust/cubesql && \
     export CUBESQL_SQL_PUSH_DOWN=true && \
     export CUBESQL_REWRITE_CACHE=true && \
     export CUBESQL_REWRITE_TIMEOUT=60 && \
     cargo insta test --all-features --workspace --unreferenced warn"

# ============================================
# 3. NATIVE BUILD & TESTS
# ============================================

echo -e "${YELLOW}=== NATIVE BUILD & TESTS ===${NC}"
echo ""

# Ensure dependencies are installed
run_test "Yarn install" \
    "cd $CUBE_ROOT && yarn install --frozen-lockfile"

run_test "Lerna tsc" \
    "cd $CUBE_ROOT && yarn tsc"

run_test "Build native (debug)" \
    "cd $CUBE_ROOT/packages/cubejs-backend-native && yarn run native:build-debug"

run_test "Native unit tests" \
    "cd $CUBE_ROOT/packages/cubejs-backend-native && \
     export CUBESQL_STREAM_MODE=true && \
     export CUBEJS_NATIVE_INTERNAL_DEBUG=true && \
     yarn run test:unit"

# ============================================
# 4. E2E SMOKE TESTS
# ============================================

echo -e "${YELLOW}=== E2E SMOKE TESTS ===${NC}"
echo ""

run_test "E2E Smoke testing over whole Cube" \
    "cd $CUBE_ROOT/packages/cubejs-testing && \
     export CUBEJS_NATIVE_INTERNAL_DEBUG=true && \
     yarn smoke:cubesql"

# ============================================
# SUMMARY
# ============================================

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}TEST SUMMARY${NC}"
echo -e "${BLUE}========================================${NC}"

if [ $FAILURES -eq 0 ]; then
    echo -e "${GREEN}✓ All tests passed!${NC}"
    echo ""
    echo "You can commit and push with confidence!"
    exit 0
else
    echo -e "${RED}✗ $FAILURES test(s) failed${NC}"
    echo ""
    echo "Please fix the failing tests before committing."
    exit 1
fi
