# Local CI Testing Scripts

This directory contains scripts to run the same tests that GitHub CI runs, allowing you to test locally before committing and pushing.

## Available Scripts

### 1. 🚀 Quick Pre-Commit Checks (1-2 minutes)

```bash
./run-quick-checks.sh
```

**What it does:**
- ✓ Rust formatting checks (all packages)
- ✓ Clippy linting (CubeSQL only)
- ✓ Unit tests (CubeSQL only)

**When to use:** Before every commit to catch the most common issues quickly.

---

### 2. 🔧 Fix Formatting

```bash
./fix-formatting.sh
```

**What it does:**
- Automatically formats all Rust code using `cargo fmt`
- Fixes: CubeSQL, Native, cubenativeutils, cubesqlplanner

**When to use:** When formatting checks fail, run this first.

---

### 3. 🔍 Clippy Only (2-3 minutes)

```bash
./run-clippy.sh
```

**What it does:**
- ✓ Runs clippy on all Rust packages
- ✓ Checks for code quality issues and warnings
- ✓ Tests both with and without Python feature

**When to use:** To check for code quality issues without running tests.

---

### 4. 🧪 Tests Only (5-10 minutes)

```bash
./run-tests-only.sh
```

**What it does:**
- ✓ CubeSQL unit tests (with insta snapshots)
- ✓ Native unit tests (if built)

**When to use:** When you've already formatted/linted and just want to run tests.

---

### 5. 🏁 Full CI Tests (15-30 minutes)

```bash
./run-ci-tests-local.sh
```

**What it does:**
- ✓ All formatting checks (fmt)
- ✓ All linting checks (clippy on all packages)
- ✓ All unit tests (CubeSQL with Rewrite Engine)
- ✓ Native build (debug mode)
- ✓ Native unit tests
- ✓ E2E smoke tests

**When to use:** Before pushing to GitHub, especially for important commits.

---

## Recommended Workflow

### Before Every Commit:
```bash
# 1. Fix formatting
./fix-formatting.sh

# 2. Run quick checks
./run-quick-checks.sh
```

### Before Pushing:
```bash
# Run full CI tests
./run-ci-tests-local.sh
```

### When Debugging Specific Issues:
```bash
# Just formatting
./fix-formatting.sh

# Just linting
./run-clippy.sh

# Just tests
./run-tests-only.sh
```

---

## What GitHub CI Tests

The `run-ci-tests-local.sh` script mirrors the GitHub Actions workflow defined in:
```
.github/workflows/rust-cubesql.yml
```

**GitHub CI Jobs:**
1. **Lint** - Format and clippy checks for all Rust packages
2. **Unit** - Unit tests with code coverage (Rewrite Engine)
3. **Native Linux** - Build and test native packages
4. **Native macOS** - Build and test on macOS (not in local script)
5. **Native Windows** - Build and test on Windows (not in local script)

---

## Prerequisites

### Required:
- Rust toolchain (1.90.0+)
- Cargo
- Node.js (22.x)
- Yarn

### Auto-installed by scripts:
- `cargo-insta` (for snapshot testing)
- `cargo-llvm-cov` (for code coverage - only in full CI tests)

---

## Common Issues

### "cargo-insta not found"
The scripts will automatically install it on first run.

### Native tests skipped
Run this first:
```bash
cd packages/cubejs-backend-native
yarn run native:build-debug
```

### Tests fail with "Connection refused"
Make sure you're not running other Cube instances on the test ports.

### Clippy warnings
Fix or allow them using `#[allow(clippy::warning_name)]` if appropriate.

---

## Environment Variables

The scripts set the same environment variables as GitHub CI:

```bash
# Unit tests
CUBESQL_SQL_PUSH_DOWN=true
CUBESQL_REWRITE_CACHE=true
CUBESQL_REWRITE_TIMEOUT=60

# Native tests
CUBESQL_STREAM_MODE=true
CUBEJS_NATIVE_INTERNAL_DEBUG=true
```

---

## Exit Codes

- **0** - All tests passed
- **1** - One or more tests failed

Scripts stop on first failure (set -e), so you can fix issues incrementally.

---

## Tips

1. **Speed up testing:** Run `run-quick-checks.sh` frequently, `run-ci-tests-local.sh` before pushing.

2. **Watch mode:** For active development, use:
   ```bash
   cd rust/cubesql
   cargo watch -x test
   ```

3. **Individual tests:** Run specific tests with:
   ```bash
   cd rust/cubesql
   cargo test test_name
   ```

4. **Update snapshots:** When tests fail due to expected changes:
   ```bash
   cd rust/cubesql
   cargo insta review
   ```

---

## Troubleshooting

### Slow tests
- First run downloads dependencies (slow)
- Subsequent runs use Cargo cache (fast)
- Consider `cargo clean` if builds seem stale

### Out of memory
- Close other applications
- Reduce parallelism: `cargo test -- --test-threads=1`

### Stale cache
```bash
cargo clean
rm -rf target/
```

---

## Integration with Git Hooks

You can set up automatic pre-commit checks:

```bash
# In .git/hooks/pre-commit
#!/bin/bash
cd examples/recipes/arrow-ipc
./run-quick-checks.sh
```

Make it executable:
```bash
chmod +x .git/hooks/pre-commit
```

Now checks run automatically before every commit!

---

**Version:** 1.0
**Last Updated:** 2024-12-27
**Compatibility:** Matches GitHub Actions `rust-cubesql.yml` workflow
