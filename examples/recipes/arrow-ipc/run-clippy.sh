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
echo -e "${BLUE}Running Clippy (Rust Linter)${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

FAILURES=0

run_clippy() {
    local name="$1"
    local dir="$2"
    local extra_flags="$3"

    echo -e "${BLUE}>>> Clippy: $name${NC}"
    if cd "$dir" && cargo clippy --locked --workspace --all-targets --keep-going $extra_flags -- -D warnings; then
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
# RUN CLIPPY ON ALL COMPONENTS
# ============================================

run_clippy "CubeSQL" \
    "$CUBE_ROOT/rust/cubesql" \
    ""

run_clippy "Native" \
    "$CUBE_ROOT/packages/cubejs-backend-native" \
    ""

run_clippy "Native (with Python)" \
    "$CUBE_ROOT/packages/cubejs-backend-native" \
    "--features python"

run_clippy "cubenativeutils" \
    "$CUBE_ROOT/rust/cubenativeutils" \
    ""

run_clippy "cubesqlplanner" \
    "$CUBE_ROOT/rust/cubesqlplanner" \
    ""

# ============================================
# SUMMARY
# ============================================

echo ""
echo -e "${BLUE}========================================${NC}"

if [ $FAILURES -eq 0 ]; then
    echo -e "${GREEN}✓ All clippy checks passed!${NC}"
    exit 0
else
    echo -e "${RED}✗ $FAILURES clippy check(s) failed${NC}"
    echo ""
    echo "Please fix the clippy warnings before committing."
    exit 1
fi
