# Development Guide

## Running GitHub Actions Locally

### Check fmt/clippy

Run the exact same checks that GitHub Actions runs in the "Check fmt/clippy" job:

```bash
./scripts/check-fmt-clippy.sh
```

This script checks:

#### Formatting (cargo fmt)
- ✅ CubeSQL (`rust/cubesql`)
- ✅ Backend Native (`packages/cubejs-backend-native`)
- ✅ Cube Native Utils (`rust/cubenativeutils`)
- ✅ CubeSQL Planner (`rust/cubesqlplanner`)

#### Linting (cargo clippy)
- ✅ CubeSQL
- ✅ Backend Native
- ✅ Backend Native (with Python features)
- ✅ Cube Native Utils
- ✅ CubeSQL Planner

### Individual Commands

Run specific checks manually:

#### Format Check (specific crate)
```bash
cd rust/cubesql
cargo fmt --all -- --check
```

#### Format Fix (specific crate)
```bash
cd rust/cubesql
cargo fmt --all
```

#### Clippy Check (specific crate)
```bash
cd rust/cubesql
cargo clippy --locked --workspace --all-targets --keep-going -- -D warnings
```

#### All Rust Crates at Once
```bash
# Format all
for dir in rust/cubesql packages/cubejs-backend-native rust/cubenativeutils rust/cubesqlplanner; do
  cd "$dir" && cargo fmt --all && cd -
done

# Check all
for dir in rust/cubesql packages/cubejs-backend-native rust/cubenativeutils rust/cubesqlplanner; do
  cd "$dir" && cargo clippy --locked --workspace --all-targets --keep-going -- -D warnings && cd -
done
```

## Pre-commit Hook (Optional)

Create `.git/hooks/pre-commit` to automatically run checks before committing:

```bash
#!/bin/bash
# Pre-commit hook to run fmt/clippy checks

echo "Running pre-commit checks..."

# Run the check script
./scripts/check-fmt-clippy.sh

# If checks fail, prevent commit
if [ $? -ne 0 ]; then
  echo ""
  echo "Pre-commit checks failed!"
  echo "Please fix the errors and try again."
  echo ""
  echo "To bypass this hook (not recommended), use:"
  echo "  git commit --no-verify"
  exit 1
fi

echo "Pre-commit checks passed!"
exit 0
```

Make it executable:

```bash
chmod +x .git/hooks/pre-commit
```

## Common Issues

### Issue: Formatting Differences

**Problem**: `cargo fmt` shows differences but you didn't change the file.

**Solution**: Different Rust versions may format differently. The CI uses Rust 1.90.0:

```bash
rustup install 1.90.0
rustup default 1.90.0
```

### Issue: Clippy Warnings

**Problem**: Clippy shows warnings with `-D warnings` flag.

**Solution**: Fix the warnings. Common fixes:

```bash
# Remove unused imports
# Comment out or remove: use super::*;

# Fix unused variables
# Prefix with underscore: let _unused = value;

# Fix deprecated syntax
# Change: 'localhost' to ~c"localhost"
```

### Issue: Python Feature Not Available

**Problem**: `cargo clippy --features python` fails.

**Solution**: Install Python development headers:

```bash
# Ubuntu/Debian
sudo apt install python3-dev

# macOS
brew install python@3.11

# Set Python version
export PYO3_PYTHON=python3.11
```

### Issue: Locked Flag Fails

**Problem**: `--locked` flag fails with dependency changes.

**Solution**: Update Cargo.lock:

```bash
cd rust/cubesql
cargo update
git add Cargo.lock
```

## Workflow Integration

### Before Pushing
```bash
# 1. Format your code
cargo fmt --all

# 2. Run checks
./scripts/check-fmt-clippy.sh

# 3. Fix any issues
# 4. Commit and push
```

### During Development
```bash
# Quick check while coding
cargo clippy

# Auto-fix some issues
cargo fix --allow-dirty

# Format on save (VS Code)
# Add to .vscode/settings.json:
{
  "rust-analyzer.rustfmt.extraArgs": ["--edition=2021"],
  "[rust]": {
    "editor.formatOnSave": true
  }
}
```

## CI/CD Pipeline

The GitHub Actions workflow (`.github/workflows/rust-cubesql.yml`) runs:

1. **Lint Job** (20 min timeout)
   - Runs on: `ubuntu-24.04`
   - Container: `cubejs/rust-cross:x86_64-unknown-linux-gnu-15082024`
   - Rust version: `1.90.0`
   - Components: `rustfmt`, `clippy`

2. **Unit Tests** (60 min timeout)
   - Runs snapshot tests with `cargo-insta`
   - Generates code coverage

3. **Native Builds**
   - Linux (GNU): x86_64, aarch64
   - macOS: x86_64, aarch64
   - Windows: x86_64
   - With Python: 3.9, 3.10, 3.11, 3.12

## Additional Resources

- **Workflow file**: `.github/workflows/rust-cubesql.yml`
- **Rust toolchain**: `1.90.0` (matches CI)
- **Container image**: `cubejs/rust-cross:x86_64-unknown-linux-gnu-15082024`

## Quick Reference

```bash
# Run all checks (GitHub Actions equivalent)
./scripts/check-fmt-clippy.sh

# Format all code
find rust packages -name Cargo.toml -exec dirname {} \; | xargs -I {} sh -c 'cd {} && cargo fmt --all'

# Check single crate
cd rust/cubesql && cargo clippy --locked --workspace --all-targets -- -D warnings

# Fix common issues
cargo fix --allow-dirty
cargo fmt --all
```
