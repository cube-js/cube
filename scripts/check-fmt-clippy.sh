#!/bin/bash
#
# Run GitHub Actions "Check fmt/clippy" locally
# This replicates the lint job from .github/workflows/rust-cubesql.yml
#

set -e  # Exit on error

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}╔══════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  Running GitHub Actions: Check fmt/clippy locally   ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════════════╝${NC}"

# Change to repo root
cd "$(dirname "$0")/.."
REPO_ROOT=$(pwd)

# Track failures
FAILED=0

# Function to run a check
run_check() {
  local name="$1"
  local dir="$2"
  local cmd="$3"

  echo -e "\n${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
  echo -e "${BLUE}▶ $name${NC}"
  echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
  echo -e "  Directory: $dir"
  echo -e "  Command: $cmd"
  echo ""

  cd "$REPO_ROOT/$dir"

  if eval "$cmd"; then
    echo -e "${GREEN}✅ $name passed${NC}"
  else
    echo -e "${RED}❌ $name failed${NC}"
    FAILED=$((FAILED + 1))
  fi

  cd "$REPO_ROOT"
}

echo -e "\n${BLUE}════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}  FORMATTING CHECKS (cargo fmt)${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════${NC}"

# Formatting checks
run_check "Lint CubeSQL" "rust/cubesql" "cargo fmt --all -- --check"
run_check "Lint Native" "packages/cubejs-backend-native" "cargo fmt --all -- --check"
run_check "Lint cubenativeutils" "rust/cubenativeutils" "cargo fmt --all -- --check"
run_check "Lint cubesqlplanner" "rust/cubesqlplanner" "cargo fmt --all -- --check"

echo -e "\n${BLUE}════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}  CLIPPY CHECKS (cargo clippy)${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════${NC}"

# Clippy checks
run_check "Clippy CubeSQL" "rust/cubesql" "cargo clippy --locked --workspace --all-targets --keep-going -- -D warnings"
run_check "Clippy Native" "packages/cubejs-backend-native" "cargo clippy --locked --workspace --all-targets --keep-going -- -D warnings"
run_check "Clippy Native (with Python)" "packages/cubejs-backend-native" "cargo clippy --locked --workspace --all-targets --keep-going --features python -- -D warnings"
run_check "Clippy cubenativeutils" "rust/cubenativeutils" "cargo clippy --locked --workspace --all-targets --keep-going -- -D warnings"
run_check "Clippy cubesqlplanner" "rust/cubesqlplanner" "cargo clippy --locked --workspace --all-targets --keep-going -- -D warnings"

# Summary
echo -e "\n${BLUE}════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}  SUMMARY${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════${NC}"

if [ $FAILED -eq 0 ]; then
  echo -e "${GREEN}✅ All checks passed!${NC}"
  echo -e "${GREEN}   Your code is ready for GitHub Actions.${NC}"
  exit 0
else
  echo -e "${RED}❌ $FAILED check(s) failed${NC}"
  echo -e "${RED}   Please fix the errors before pushing.${NC}"
  exit 1
fi
