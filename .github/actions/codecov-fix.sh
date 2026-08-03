#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../" && pwd)"

for path in "$repo_root"/packages/*/coverage/lcov.info; do
  [ -f "$path" ] || continue

  pkg_dir="$(dirname "$path")"          # packages/foo/coverage
  pkg_root="$(dirname "$pkg_dir")"      # packages/foo
  tmp_path="$pkg_dir/lcov.fixed.info"

  sed "s|SF:src/|SF:packages/$(basename "$pkg_root")/src/|g" "$path" > "$tmp_path"
done
