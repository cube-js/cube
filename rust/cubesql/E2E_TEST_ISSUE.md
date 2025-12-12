# E2E Test Issue: Unreferenced Snapshots

## Problem Summary

The GitHub Actions "Unit (Rewrite Engine)" job is failing with unreferenced snapshot errors:

```
warning: encountered unreferenced snapshots:
  e2e__tests__postgres__system_pg_catalog.pg_tables.snap
  e2e__tests__postgres__pg_test_types.snap
  e2e__tests__postgres__system_information_schema.columns.snap
  e2e__tests__postgres__select_count(asterisk)_count_status_from_orders_group_by_status_order_by_count_desc.snap
  e2e__tests__postgres__system_pg_catalog.pg_type.snap
  e2e__tests__postgres__system_pg_catalog.pg_class.snap
  e2e__tests__postgres__datepart_quarter.snap
  e2e__tests__postgres__system_information_schema.tables.snap
  e2e__tests__postgres__system_pg_catalog.pg_proc.snap
error: aborting because of unreferenced snapshots
```

## Root Cause

The issue occurs because:

1. **E2E tests require Cube server credentials** stored as GitHub secrets:
   - `CUBESQL_TESTING_CUBE_TOKEN`
   - `CUBESQL_TESTING_CUBE_URL`

2. **When secrets are missing/invalid**:
   - Locally: Tests are skipped → snapshots become "unreferenced"
   - In CI: Tests may fail or skip → snapshots become "unreferenced"

3. **The `--unreferenced reject` flag** causes the build to fail when snapshots aren't used

## Why Master Works But Feature Branch Fails

### Possible Reasons:

1. **Secrets not configured for fork/branch**:
   - GitHub secrets are repository-specific
   - Forks don't inherit secrets from upstream
   - Feature branches may not have access to organization secrets

2. **Cube server connectivity issues**:
   - The Cube test server might be down
   - Network/firewall issues preventing access
   - Credentials might have expired

3. **Test execution order**:
   - Recent changes might affect when/how e2e tests run
   - Timing issues with test startup

## Solutions

### Option 1: Fix Secret Access (Recommended for CI)

Ensure GitHub secrets are properly configured:

```bash
# In GitHub repository settings → Secrets and variables → Actions
# Add these secrets:
CUBESQL_TESTING_CUBE_TOKEN=<token>
CUBESQL_TESTING_CUBE_URL=<url>
```

### Option 2: Make Snapshots Optional

Modify the workflow to allow unreferenced snapshots:

```yaml
# In .github/workflows/rust-cubesql.yml line 109
# Change from:
cargo insta test --all-features --workspace --unreferenced reject

# To:
cargo insta test --all-features --workspace --unreferenced warn
```

This will warn about unreferenced snapshots but won't fail the build.

### Option 3: Conditional E2E Tests

Update the workflow to skip e2e tests when secrets aren't available:

```yaml
- name: Unit tests (Rewrite Engine)
  env:
    CUBESQL_TESTING_CUBE_TOKEN: ${{ secrets.CUBESQL_TESTING_CUBE_TOKEN }}
    CUBESQL_TESTING_CUBE_URL: ${{ secrets.CUBESQL_TESTING_CUBE_URL }}
    CUBESQL_SQL_PUSH_DOWN: true
    CUBESQL_REWRITE_CACHE: true
    CUBESQL_REWRITE_TIMEOUT: 60
  run: |
    cd rust/cubesql
    source <(cargo llvm-cov show-env --export-prefix)
    # Skip --unreferenced reject if secrets aren't set
    if [ -z "$CUBESQL_TESTING_CUBE_TOKEN" ]; then
      cargo insta test --all-features --workspace --unreferenced warn
    else
      cargo insta test --all-features --workspace --unreferenced reject
    fi
    cargo llvm-cov report --lcov --output-path lcov.info
```

### Option 4: Remove Snapshots Temporarily

If you can't fix secrets immediately, temporarily remove the snapshots:

```bash
cd rust/cubesql
rm cubesql/e2e/tests/snapshots/*.snap
git commit -am "temp: remove e2e snapshots until secrets are configured"
```

The snapshots will be regenerated when the e2e tests run successfully with proper credentials.

## How to Test Locally

### Without Credentials (Tests Skip)
```bash
cd rust/cubesql
cargo insta test --all-features --workspace --unreferenced warn
# Status: Tests pass, snapshots show as unreferenced
```

### With Dummy Credentials (Tests Fail)
```bash
CUBESQL_TESTING_CUBE_TOKEN=dummy \
CUBESQL_TESTING_CUBE_URL=http://dummy \
cargo test --package cubesql --test e2e
# Status: Tests fail trying to connect to Cube server
```

### With Valid Credentials (Tests Pass)
```bash
CUBESQL_TESTING_CUBE_TOKEN=<real-token> \
CUBESQL_TESTING_CUBE_URL=<real-url> \
cargo insta test --all-features --workspace --unreferenced reject
# Status: All tests pass, snapshots are used
```

## Affected Files

- **Test file**: `cubesql/e2e/tests/postgres.rs` (lines 1182-1259)
- **Snapshots**: `cubesql/e2e/tests/snapshots/e2e__tests__postgres__*.snap`
- **Workflow**: `.github/workflows/rust-cubesql.yml` (line 109)

## Recommendation

**For fork/feature branch development**:
Use Option 2 (change to `--unreferenced warn`) to allow development without Cube server access.

**For main repository**:
Use Option 1 (fix secrets) to ensure e2e tests run and snapshots are validated.

## Related Commits

- `5a183251b` - "restore masters e2e" - Added the snapshots
- Last workflow update: `521c47e5f` (v1.5.14 branch point)
