#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CUBE_ROOT="$SCRIPT_DIR/../../.."

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Fixing Rust Formatting${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

echo -e "${YELLOW}Formatting CubeSQL...${NC}"
cd "$CUBE_ROOT/rust/cubesql" && cargo fmt --all
echo -e "${GREEN}✓ CubeSQL formatted${NC}"

echo -e "${YELLOW}Formatting Native...${NC}"
cd "$CUBE_ROOT/packages/cubejs-backend-native" && cargo fmt --all
echo -e "${GREEN}✓ Native formatted${NC}"

echo -e "${YELLOW}Formatting cubenativeutils...${NC}"
cd "$CUBE_ROOT/rust/cubenativeutils" && cargo fmt --all
echo -e "${GREEN}✓ cubenativeutils formatted${NC}"

echo -e "${YELLOW}Formatting cubesqlplanner...${NC}"
cd "$CUBE_ROOT/rust/cubesqlplanner" && cargo fmt --all
echo -e "${GREEN}✓ cubesqlplanner formatted${NC}"

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}✓ All Rust code formatted!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "You can now commit your changes."
