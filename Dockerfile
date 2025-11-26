# Multi-stage Dockerfile for building complete Cube application
# with all JavaScript packages, Rust components, and PostgreSQL compatibility fixes

# Stage 1: Builder for Cube JavaScript/TypeScript packages
FROM node:20-bookworm as js-builder

WORKDIR /workspace

# Copy root-level configuration files
COPY package.json yarn.lock lerna.json ./
COPY packages/ packages/

# Install dependencies
RUN yarn install --frozen-lockfile

# Build all packages
RUN yarn build

# Stage 2: Builder for Rust components (CubeSQL, CubeStore)
FROM docker.io/library/rust:1.90.0 as rust-builder

WORKDIR /workspace

# Install Rust build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    libpq-dev \
    git \
    curl \
    build-essential \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy Rust source code
COPY rust/cubeshared /workspace/rust/cubeshared
COPY rust/cubesqlplanner /workspace/rust/cubesqlplanner
COPY rust/cubesql /workspace/rust/cubesql

# Build CubeSQL in release mode
WORKDIR /workspace/rust/cubesql
RUN cargo build --release -p cubesql

# Stage 3: Runtime image
FROM node:20-bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libssl3 \
    ca-certificates \
    libpq5 \
    postgresql-client \
    dumb-init \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy built JavaScript packages from js-builder
COPY --from=js-builder /workspace/packages ./packages
COPY --from=js-builder /workspace/node_modules ./node_modules

# Copy package.json files needed at runtime
COPY --from=js-builder /workspace/package.json ./
COPY --from=js-builder /workspace/yarn.lock ./
COPY --from=js-builder /workspace/lerna.json ./

# Copy CubeSQL PostgreSQL server binary from rust-builder
COPY --from=rust-builder /workspace/rust/cubesql/target/release/cubesqld /app/cubesqld

# Create non-root user for security
RUN useradd -m -u 1001 -g root cube && \
    chown -R cube:root /app

USER cube

# Expose ports
# 4000: Cube API
# 5432: PostgreSQL/CubeSQL
EXPOSE 4000 5432

# Set environment variables
ENV RUST_LOG=info
ENV RUST_BACKTRACE=1
ENV NODE_ENV=production

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD node -e "require('http').get('http://localhost:4000/api/v1/info', (r) => {if (r.statusCode !== 200) throw new Error(r.statusCode)})" || exit 1

# Use dumb-init to handle signals properly
ENTRYPOINT ["/usr/bin/dumb-init", "--"]

# Default command - start Cube API server
# Override this to start CubeSQL: /app/cubesqld
CMD ["node", "packages/cubejs-server/bin/cubejs-server"]
