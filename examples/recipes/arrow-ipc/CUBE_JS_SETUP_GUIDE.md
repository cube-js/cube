# Comprehensive Cube.js Setup Guide

Complete guide for setting up and running Cube.js from your current development branch.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Quick Start](#quick-start)
3. [Development Setups](#development-setups)
4. [Configuration](#configuration)
5. [Running Tests](#running-tests)
6. [Troubleshooting](#troubleshooting)
7. [Advanced Topics](#advanced-topics)

---

## Prerequisites

### Required Tools

- **Node.js**: v18+ (check with `node --version`)
- **Yarn**: v1.22.19+ (check with `yarn --version`)
- **Rust**: 1.90.0+ (for CubeSQL components, check with `rustc --version`)
- **Git**: For version control

### Optional Tools

- **Docker**: For containerized development
- **PostgreSQL Client** (`psql`): For testing database connections
- **DuckDB CLI**: For lightweight database testing

### Verify Installation

```bash
node --version      # Should be v18+
yarn --version      # Should be v1.22.19+
rustc --version     # Should be 1.90.0+
cargo --version     # Should match rustc version
```

---

## Quick Start

### 1. Clone and Install

```bash
# Navigate to your cube repository
cd /path/to/cube

# Install all dependencies (may take 5-10 minutes)
yarn install

# Verify installation
yarn --version
```

### 2. Build TypeScript Packages

```bash
# Compile all TypeScript packages
yarn tsc

# Or watch for changes during development
yarn tsc:watch
```

### 3. Build Native Components

```bash
# Navigate to backend-native package
cd packages/cubejs-backend-native

# Build debug version (recommended for development)
yarn run native:build-debug

# Link package globally for local development
yarn link

# Return to root
cd ../..
```

### 4. Create a Test Project

```bash
# Option A: Use existing example
cd examples/recipes/changing-visibility-of-cubes-or-views
yarn install
yarn link "@cubejs-backend/native"
yarn dev

# Option B: Create minimal project
mkdir ~/cube-dev-test
cd ~/cube-dev-test

cat > package.json <<'EOF'
{
  "name": "cube-dev-test",
  "private": true,
  "scripts": {
    "dev": "cubejs-server",
    "build": "cubejs build"
  },
  "devDependencies": {
    "@cubejs-backend/server": "*",
    "@cubejs-backend/duckdb-driver": "*"
  }
}
EOF

yarn install
yarn link "@cubejs-backend/native"
yarn dev
```

### 5. Access Cube.js

- **Developer Playground**: http://localhost:4000
- **API Endpoint**: http://localhost:4000/cubejs-api
- **Default Port**: 4000

---

## Development Setups

### Setup A: Local Development (No Database Required)

**Best for:** Quick testing, simple schema development, prototyping

#### Step 1: Initialize Project

```bash
mkdir cube-local-dev
cd cube-local-dev

cat > package.json <<'EOF'
{
  "name": "cube-local-dev",
  "private": true,
  "scripts": {
    "dev": "cubejs-server",
    "build": "cubejs build",
    "start": "node index.js"
  },
  "devDependencies": {
    "@cubejs-backend/duckdb-driver": "*",
    "@cubejs-backend/server": "*"
  }
}
EOF

cat > cube.js <<'EOF'
module.exports = {
  processUnnestArrayWithLabel: true,
  checkAuth: (ctx, auth) => {
    console.log('Auth context:', auth);
  }
};
EOF

mkdir schema
cat > schema/Orders.js <<'EOF'
cube(`Orders`, {
  sql: `SELECT * FROM (
    SELECT 1 as id, 'pending' as status, 100 as amount
    UNION ALL
    SELECT 2 as id, 'completed' as status, 200 as amount
    UNION ALL
    SELECT 3 as id, 'pending' as status, 150 as amount
  )`,

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true
    },
    status: {
      sql: `status`,
      type: `string`
    }
  },

  measures: {
    count: {
      type: `count`
    },
    totalAmount: {
      sql: `amount`,
      type: `sum`
    }
  }
});
EOF
```

#### Step 2: Link Dependencies

```bash
# Link your local backend-native
yarn link "@cubejs-backend/native"

# Install remaining dependencies
yarn install
```

#### Step 3: Run Development Server

```bash
# Start Cube.js
yarn dev

# Output should show:
# ✓ Cube.js server is running
# ✓ API: http://localhost:4000/cubejs-api
# ✓ Playground: http://localhost:4000
```

#### Step 4: Test with curl or API client

```bash
# Get API token (development mode generates one automatically)
curl http://localhost:4000/cubejs-api/v1/load \
  -H "Authorization: Bearer test-token" \
  -H "Content-Type: application/json" \
  -d '{
    "query": {
      "measures": ["Orders.count"],
      "timeDimensions": [],
      "dimensions": ["Orders.status"]
    }
  }'
```

---

### Setup B: PostgreSQL Development

**Best for:** Testing with real databases, complex schemas, production-like testing

#### Step 1: Start PostgreSQL

```bash
# Option 1: Using Docker
docker run -d \
  --name cube-postgres \
  -e POSTGRES_USER=cubejs \
  -e POSTGRES_PASSWORD=password123 \
  -e POSTGRES_DB=cubejs_dev \
  -p 5432:5432 \
  postgres:14

# Option 2: Using existing PostgreSQL
# Ensure it's running on localhost:5432

# Option 3: Using Homebrew (macOS)
brew services start postgresql@14
createuser -P cubejs  # Set password: password123
createdb -O cubejs cubejs_dev
```

#### Step 2: Create Sample Data

```bash
# Connect to PostgreSQL
psql -h localhost -U cubejs -d cubejs_dev

# Create test tables
CREATE TABLE orders (
  id SERIAL PRIMARY KEY,
  status VARCHAR(50),
  amount DECIMAL(10, 2),
  created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE users (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255),
  email VARCHAR(255)
);

-- Insert sample data
INSERT INTO orders (status, amount) VALUES
('pending', 100),
('completed', 200),
('pending', 150),
('completed', 300),
('failed', 50);

INSERT INTO users (name, email) VALUES
('John Doe', 'john@example.com'),
('Jane Smith', 'jane@example.com');

\q  # Exit psql
```

#### Step 3: Create Cube.js Project

```bash
mkdir cube-postgres-dev
cd cube-postgres-dev

cat > package.json <<'EOF'
{
  "name": "cube-postgres-dev",
  "private": true,
  "scripts": {
    "dev": "cubejs-server",
    "build": "cubejs build"
  },
  "devDependencies": {
    "@cubejs-backend/postgres-driver": "*",
    "@cubejs-backend/server": "*"
  }
}
EOF

cat > .env <<'EOF'
CUBEJS_DB_TYPE=postgres
CUBEJS_DB_HOST=localhost
CUBEJS_DB_PORT=5432
CUBEJS_DB_USER=cubejs
CUBEJS_DB_PASS=password123
CUBEJS_DB_NAME=cubejs_dev
CUBEJS_DEV_MODE=true
CUBEJS_LOG_LEVEL=debug
NODE_ENV=development
EOF

mkdir schema
cat > schema/Orders.js <<'EOF'
cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true
    },
    status: {
      sql: `status`,
      type: `string`
    },
    createdAt: {
      sql: `created_at`,
      type: `time`
    }
  },

  measures: {
    count: {
      type: `count`
    },
    totalAmount: {
      sql: `amount`,
      type: `sum`
    },
    avgAmount: {
      sql: `amount`,
      type: `avg`
    }
  }
});
EOF

cat > schema/Users.js <<'EOF'
cube(`Users`, {
  sql: `SELECT * FROM public.users`,

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true
    },
    name: {
      sql: `name`,
      type: `string`
    },
    email: {
      sql: `email`,
      type: `string`
    }
  },

  measures: {
    count: {
      type: `count`
    }
  }
});
EOF
```

#### Step 4: Link and Run

```bash
yarn link "@cubejs-backend/native"
yarn install
yarn dev
```

#### Step 5: Test Database Connection

```bash
# The playground should show Orders and Users cubes
# http://localhost:4000

# Test via API
curl http://localhost:4000/cubejs-api/v1/load \
  -H "Authorization: Bearer test-token" \
  -H "Content-Type: application/json" \
  -d '{
    "query": {
      "measures": ["Orders.count", "Orders.totalAmount"],
      "dimensions": ["Orders.status"]
    }
  }'

# Expected response:
# {
#   "data": [
#     {"Orders.status": "pending", "Orders.count": 2, "Orders.totalAmount": 250},
#     {"Orders.status": "completed", "Orders.count": 2, "Orders.totalAmount": 500},
#     {"Orders.status": "failed", "Orders.count": 1, "Orders.totalAmount": 50}
#   ]
# }
```

---

### Setup C: Docker Compose (Complete Stack)

**Best for:** Testing across multiple services, reproducible environments, team collaboration

#### Step 1: Create Project Structure

```bash
mkdir cube-docker-dev
cd cube-docker-dev

cat > docker-compose.yml <<'EOF'
version: '3.8'

services:
  postgres:
    image: postgres:14-alpine
    container_name: cube-postgres
    environment:
      POSTGRES_USER: cubejs
      POSTGRES_PASSWORD: password123
      POSTGRES_DB: cubejs_dev
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./init.sql:/docker-entrypoint-initdb.d/init.sql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U cubejs"]
      interval: 10s
      timeout: 5s
      retries: 5

  cube:
    build:
      context: ../..  # Cube repository root
      dockerfile: packages/cubejs-docker/Dockerfile
    container_name: cube-server
    environment:
      CUBEJS_DB_TYPE: postgres
      CUBEJS_DB_HOST: postgres
      CUBEJS_DB_USER: cubejs
      CUBEJS_DB_PASS: password123
      CUBEJS_DB_NAME: cubejs_dev
      CUBEJS_DEV_MODE: "true"
      CUBEJS_LOG_LEVEL: debug
      NODE_ENV: development
    ports:
      - "4000:4000"
      - "3000:3000"
    volumes:
      - .:/cube/conf
      - .empty:/cube/conf/node_modules/@cubejs-backend/
    depends_on:
      postgres:
        condition: service_healthy
    command: cubejs-server

volumes:
  postgres_data:

  .empty:
    driver: local
EOF

cat > init.sql <<'EOF'
CREATE TABLE orders (
  id SERIAL PRIMARY KEY,
  status VARCHAR(50),
  amount DECIMAL(10, 2),
  created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE users (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255),
  email VARCHAR(255)
);

INSERT INTO orders (status, amount) VALUES
('pending', 100),
('completed', 200),
('pending', 150),
('completed', 300),
('failed', 50);

INSERT INTO users (name, email) VALUES
('John Doe', 'john@example.com'),
('Jane Smith', 'jane@example.com');
EOF

cat > cube.js <<'EOF'
module.exports = {
  processUnnestArrayWithLabel: true,
};
EOF

mkdir schema
cat > schema/Orders.js <<'EOF'
cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true
    },
    status: {
      sql: `status`,
      type: `string`
    }
  },

  measures: {
    count: {
      type: `count`
    },
    totalAmount: {
      sql: `amount`,
      type: `sum`
    }
  }
});
EOF
```

#### Step 2: Start Services

```bash
# Build and start containers
docker-compose up --build

# Output should show:
# cube-server    | ✓ Cube.js server is running
# cube-server    | ✓ API: http://localhost:4000/cubejs-api
# cube-server    | ✓ Playground: http://localhost:4000
```

#### Step 3: Access Services

```bash
# Access Cube.js Playground
open http://localhost:4000

# Connect to PostgreSQL from your machine
psql -h localhost -U cubejs -d cubejs_dev

# View logs
docker-compose logs -f cube

# Stop services
docker-compose down
```

---

### Setup D: CubeSQL E2E Testing

**Best for:** Testing CubeSQL PostgreSQL compatibility, SQL API testing

#### Step 1: Start Cube.js Server

```bash
# Use Setup B (PostgreSQL) or Setup C (Docker)
# Keep it running for the next steps

# Verify it's running
curl http://localhost:4000/cubejs-api/v1/load \
  -H "Authorization: Bearer test-token" \
  -H "Content-Type: application/json" \
  -d '{"query": {}}'
```

#### Step 2: Set Up CubeSQL E2E Tests

```bash
# From repository root
cd rust/cubesql/cubesql

# Get your Cube.js server URL and token
export CUBESQL_TESTING_CUBE_URL="http://localhost:4000/cubejs-api"
export CUBESQL_TESTING_CUBE_TOKEN="test-token"

# Run all e2e tests
cargo test --test e2e

# Run specific test
cargo test --test e2e test_cancel_simple_query

# Run with output
cargo test --test e2e -- --nocapture

# Run and review snapshots
cargo test --test e2e
cargo insta review  # If snapshots changed
```

#### Step 3: Connect with PostgreSQL Client

```bash
# In a separate terminal, start CubeSQL
# (See /rust/cubesql/CLAUDE.md for details)
CUBESQL_CUBE_URL=http://localhost:4000/cubejs-api \
CUBESQL_CUBE_TOKEN=test-token \
CUBESQL_BIND_ADDR=0.0.0.0:5432 \
cargo run --bin cubesqld

# In another terminal, connect with psql
psql -h 127.0.0.1 -p 5432 -U test -W password

# Execute SQL queries
SELECT COUNT(*) FROM Orders;
SELECT status, SUM(amount) FROM Orders GROUP BY status;
```

---

## Configuration

### Core Environment Variables

```bash
# Database Configuration
CUBEJS_DB_TYPE=postgres              # Driver type
CUBEJS_DB_HOST=localhost             # Database host
CUBEJS_DB_PORT=5432                  # Database port
CUBEJS_DB_USER=cubejs                # Database user
CUBEJS_DB_PASS=password123           # Database password
CUBEJS_DB_NAME=cubejs_dev            # Database name

# Server Configuration
CUBEJS_DEV_MODE=true                 # Enable development mode
CUBEJS_LOG_LEVEL=debug               # Log level: error, warn, info, debug
NODE_ENV=development                 # Node environment
CUBEJS_PORT=4000                     # API server port
CUBEJS_PLAYGROUND_PORT=3000          # Playground port

# API Configuration
CUBEJS_API_SECRET=my-super-secret    # API secret for JWT
CUBEJS_ENABLE_PLAYGROUND=true        # Enable playground UI
CUBEJS_ENABLE_SWAGGER_UI=true        # Enable Swagger documentation

# CubeSQL Configuration
CUBESQL_CUBE_URL=http://localhost:4000/cubejs-api
CUBESQL_CUBE_TOKEN=test-token
CUBESQL_BIND_ADDR=0.0.0.0:5432
CUBESQL_LOG_LEVEL=debug
```

### cube.js Configuration File

```javascript
// cube.js - Root configuration

module.exports = {
  // SQL parsing options
  processUnnestArrayWithLabel: true,

  // Authentication
  checkAuth: (ctx, auth) => {
    // Called for every request
    console.log('Auth info:', auth);
    if (!auth) {
      throw new Error('Authorization required');
    }
  },

  // Context function
  contextToAppId: (ctx) => {
    return ctx.userId || 'default';
  },

  // Query optimization
  queryRewrite: (query, ctx) => {
    // Modify queries before execution
    return query;
  },

  // Pre-aggregations
  preAggregationsSchema: 'public_pre_aggregations',

  // Logging
  logger: (msg, params) => {
    console.log(`[Cube] ${msg}`, params);
  }
};
```

### Schema Configuration Examples

#### Simple Dimension & Measure

```javascript
cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true
    },
    status: {
      sql: `status`,
      type: `string`,
      shown: true
    }
  },

  measures: {
    count: {
      type: `count`,
      drillMembers: [id, status]
    },
    totalAmount: {
      sql: `amount`,
      type: `sum`
    }
  }
});
```

#### With Time Dimensions

```javascript
cube(`Events`, {
  sql: `SELECT * FROM public.events`,

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true
    },
    createdAt: {
      sql: `created_at`,
      type: `time`
    }
  },

  measures: {
    count: {
      type: `count`
    }
  }
});
```

#### With Joins

```javascript
cube(`OrderUsers`, {
  sql: `
    SELECT
      o.id,
      o.user_id,
      o.amount,
      u.name
    FROM public.orders o
    JOIN public.users u ON o.user_id = u.id
  `,

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true
    },
    userName: {
      sql: `name`,
      type: `string`
    }
  },

  measures: {
    count: {
      type: `count`
    },
    totalAmount: {
      sql: `amount`,
      type: `sum`
    }
  }
});
```

---

## Running Tests

### Unit Tests

```bash
# Run all tests
yarn test

# Test specific package
cd packages/cubejs-schema-compiler
yarn test

# Watch mode
yarn test --watch

# With coverage
yarn test --coverage
```

### Build Tests

```bash
# Verify full build
yarn tsc

# Build specific package
cd packages/cubejs-server-core
yarn build
```

### Linting

```bash
# Lint all packages
yarn lint

# Fix linting issues
yarn lint:fix

# Lint package.json files
yarn lint:npm
```

### CubeSQL Tests

```bash
# Unit tests
cargo test --lib

# Integration tests
cargo test --test e2e

# Specific test
cargo test test_portal_pagination

# With backtrace
RUST_BACKTRACE=1 cargo test --test e2e

# With output
cargo test --test e2e -- --nocapture --test-threads=1

# Review snapshots
cargo insta review
```

---

## Troubleshooting

### Issue: Port Already in Use

```bash
# Find process using port 4000
lsof -i :4000

# Kill the process
kill -9 <PID>

# Or use different port
CUBEJS_PORT=4001 yarn dev
```

### Issue: Cannot Find @cubejs-backend/native

```bash
# Ensure native package is linked
cd packages/cubejs-backend-native
yarn link

# Link in your project
yarn link "@cubejs-backend/native"

# Or reinstall everything
cd /path/to/cube
rm -rf node_modules packages/*/node_modules yarn.lock
yarn install
```

### Issue: Node Version Mismatch

```bash
# Check required version
cat .nvmrc

# Use correct Node version
nvm install  # Reads .nvmrc
nvm use      # Switches to version

# Or use n
n auto
```

### Issue: TypeScript Compilation Errors

```bash
# Clean build
yarn clean

# Rebuild
yarn tsc --build --clean
yarn tsc

# Or in watch mode to see errors incrementally
yarn tsc:watch
```

### Issue: Database Connection Failed

```bash
# Verify database is running
psql -h localhost -U cubejs -d cubejs_dev -c "SELECT 1;"

# Check Cube.js logs
CUBEJS_LOG_LEVEL=debug yarn dev

# Verify environment variables
env | grep CUBEJS_DB

# Test connection manually
psql postgresql://cubejs:password123@localhost:5432/cubejs_dev
```

### Issue: CubeSQL Native Module Corruption (macOS)

```bash
cd packages/cubejs-backend-native

# Remove compiled module
rm -rf index.node native/target

# Rebuild
yarn run native:build-debug

# Test
yarn test:unit
```

### Issue: Docker Build Fails

```bash
# Verify Docker is running
docker ps

# Build with verbose output
docker-compose build --no-cache --progress=plain

# Check disk space
docker system df

# Clean up unused images
docker image prune -a
```

### Issue: Memory Limit Exceeded

```bash
# Increase Node.js memory
NODE_OPTIONS=--max-old-space-size=4096 yarn dev

# Or in Docker
# Add to docker-compose.yml:
# environment:
#   - NODE_OPTIONS=--max-old-space-size=4096
```

---

## Advanced Topics

### Custom Logger Setup

```javascript
// cube.js
module.exports = {
  logger: (msg, params) => {
    const timestamp = new Date().toISOString();
    const level = params?.level || 'info';
    console.log(`[${timestamp}] [${level}] ${msg}`, params);
  }
};
```

### Pre-aggregations Development

```javascript
// schema/Orders.js
cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,

  preAggregations: {
    statusSummary: {
      type: `rollup`,
      measureReferences: [count, totalAmount],
      dimensionReferences: [status],
      timeDimensionReference: createdAt,
      granularity: `day`,
      refreshKey: {
        every: `1 hour`
      }
    }
  },

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true
    },
    status: {
      sql: `status`,
      type: `string`
    },
    createdAt: {
      sql: `created_at`,
      type: `time`
    }
  },

  measures: {
    count: {
      type: `count`
    },
    totalAmount: {
      sql: `amount`,
      type: `sum`
    }
  }
});
```

### Security: API Token Management

```javascript
// cube.js
module.exports = {
  checkAuth: (ctx, auth) => {
    // Verify JWT token
    const token = auth?.token;
    if (!token) {
      throw new Error('Token is required');
    }

    // In production, verify with a real secret
    try {
      const decoded = jwt.verify(token, process.env.CUBEJS_API_SECRET);
      ctx.userId = decoded.sub;
      ctx.userRole = decoded.role;
    } catch (e) {
      throw new Error('Invalid token');
    }
  }
};
```

### Debugging Mode

```bash
# Enable all debug logging
CUBEJS_LOG_LEVEL=trace \
NODE_DEBUG=* \
RUST_BACKTRACE=full \
yarn dev
```

### Performance Profiling

```bash
# Node.js profiling
node --prof $(npm bin)/cubejs-server

# Analyze profile
node --prof-process isolate-*.log > profile.txt
cat profile.txt

# Or use clinic.js
npm install -g clinic
clinic doctor -- yarn dev
```

### Testing Custom Drivers

```bash
# Create test database
docker run -d \
  --name test-postgres \
  -e POSTGRES_PASSWORD=test \
  -p 5433:5432 \
  postgres:14

# Set environment
export CUBEJS_DB_TYPE=postgres
export CUBEJS_DB_HOST=localhost
export CUBEJS_DB_PORT=5433
export CUBEJS_DB_USER=postgres
export CUBEJS_DB_PASS=test

# Run tests
cd packages/cubejs-postgres-driver
yarn test
```

### Developing with Multiple Branches

```bash
# Create feature branch
git checkout -b feature/my-feature

# Make changes
# ...

# Build and test
yarn tsc
yarn test
yarn lint:fix

# Compare with main
git diff main..HEAD

# Create PR
git push origin feature/my-feature
```

---

## Next Steps

1. **Choose a setup** that matches your development needs
2. **Verify database connection** using the provided curl examples
3. **Create sample schemas** to understand Cube.js concepts
4. **Run tests** to ensure everything works
5. **Check logs** when encountering issues
6. **Refer to official docs** for advanced features

## Useful Resources

- **Official Documentation**: https://cube.dev/docs
- **API Reference**: https://cube.dev/docs/rest-api
- **Schema Guide**: https://cube.dev/docs/data-modeling/concepts
- **GitHub Issues**: https://github.com/cube-js/cube/issues
- **Community Chat**: https://slack.cube.dev

---

## Quick Reference Commands

```bash
# Install dependencies
yarn install

# Build TypeScript
yarn tsc

# Build native components
cd packages/cubejs-backend-native && yarn run native:build-debug && yarn link && cd ../..

# Start development server
yarn dev

# Run tests
yarn test

# Run linting
yarn lint:fix

# Clean build artifacts
yarn clean

# CubeSQL e2e tests (with Cube.js running)
cd rust/cubesql/cubesql && cargo test --test e2e

# Docker compose
docker-compose up --build
docker-compose down
```

---

**Last Updated**: 2025-11-27
**For Issues or Updates**: Refer to repository's CLAUDE.md files and official documentation
