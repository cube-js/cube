# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Package Overview

The `@cubejs-backend/shared` package contains shared utilities, types, and helper functions used across all Cube backend packages. This package provides core functionality like environment configuration, promise utilities, decorators, and common data types.

## Development Commands

**Note: This project uses Yarn as the package manager.**

```bash
# Build the package
yarn build

# Build with TypeScript compilation
yarn tsc

# Watch mode for development
yarn watch

# Run unit tests
yarn unit

# Run linting
yarn lint

# Fix linting issues
yarn lint:fix
```

## Architecture Overview

### Core Components

The shared package is organized into several key modules:

1. **Environment Configuration** (`src/env.ts`): Centralized environment variable management with type safety and validation
2. **Promise Utilities** (`src/promises.ts`): Async helpers including debouncing, memoization, cancellation, and retry logic
3. **Decorators** (`src/decorators.ts`): Method decorators for cross-cutting concerns like async debouncing
4. **Type Helpers** (`src/type-helpers.ts`): Common TypeScript utility types used across packages
5. **Time Utilities** (`src/time.ts`): Date/time manipulation and formatting functions
6. **Process Utilities** (`src/process.ts`): Process management and platform detection
