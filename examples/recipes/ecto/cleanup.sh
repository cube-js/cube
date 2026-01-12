#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}Cleaning up Cube development environment...${NC}"

# Kill any running cube processes
PROCS=$(ps aux | grep -E "(cubesqld|cube-api|cubestore|cubejs)" | grep -v grep | awk '{print $2}')
if [ ! -z "$PROCS" ]; then
    echo -e "${YELLOW}Stopping processes: $PROCS${NC}"
    echo "$PROCS" | xargs kill 2>/dev/null || true
    sleep 1
    # Force kill if still running
    echo "$PROCS" | xargs kill -9 2>/dev/null || true
fi

# Check for processes using our ports
for port in 3030 4008 4444 8120 7432; do
    PID=$(lsof -ti :$port 2>/dev/null)
    if [ ! -z "$PID" ]; then
        echo -e "${YELLOW}Killing process using port $port (PID: $PID)${NC}"
        kill $PID 2>/dev/null || kill -9 $PID 2>/dev/null || true
    fi
done

# Remove PID files
rm -f cube-api.pid 2>/dev/null

echo -e "${GREEN}Cleanup complete!${NC}"
echo ""
echo "You can now start fresh with:"
echo "  ./dev-start.sh"
