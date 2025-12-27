#!/bin/bash
# Setup test data for ADBC(Arrow Native) cache performance testing

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DB_HOST=${DB_HOST:-localhost}
DB_PORT=${DB_PORT:-7432}
DB_NAME=${DB_NAME:-pot_examples_dev}
DB_USER=${DB_USER:-postgres}
DB_PASS=${DB_PASS:-postgres}

echo "Setting up test data for ADBC(Arrow Native) performance tests..."
echo ""
echo "Database connection:"
echo "  Host: $DB_HOST"
echo "  Port: $DB_PORT"
echo "  Database: $DB_NAME"
echo "  User: $DB_USER"
echo ""

# Check if database is running
if ! PGPASSWORD=$DB_PASS psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d postgres -c "SELECT 1" > /dev/null 2>&1; then
    echo "Error: Cannot connect to PostgreSQL database"
    echo "Make sure PostgreSQL is running: docker-compose up -d postgres"
    exit 1
fi

# Create database if it doesn't exist
PGPASSWORD=$DB_PASS psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d postgres -c "CREATE DATABASE $DB_NAME" 2>/dev/null || true

# Load sample data
echo "Loading sample data (3000 orders)..."
if [ -f "$SCRIPT_DIR/sample_data.sql.gz" ]; then
    gunzip -c "$SCRIPT_DIR/sample_data.sql.gz" | PGPASSWORD=$DB_PASS psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME
    echo "✓ Sample data loaded successfully"
else
    echo "Warning: sample_data.sql.gz not found, skipping data load"
fi

# Verify data
ROW_COUNT=$(PGPASSWORD=$DB_PASS psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SELECT COUNT(*) FROM public.order" 2>/dev/null || echo "0")
echo ""
echo "✓ Database ready with $ROW_COUNT orders"
echo ""
echo "Next steps:"
echo "  1. Start Cube API: ./start-cube-api.sh"
echo "  2. Start CubeSQL: ./start-cubesqld.sh"
echo "  3. Run Python tests: python test_arrow_cache_performance.py"
