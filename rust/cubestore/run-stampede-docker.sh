#!/usr/bin/env bash
# Faithful (Linux/glibc + cgroup) reproduction of the main-node disk-space-check
# memory stampede. Runs the `disk_space_stampede` bench inside the cubestore
# builder image so the allocator, malloc_trim loop, and cgroup working-set
# accounting match production.
#
#   ./run-stampede-docker.sh            # measure peak scaling (no OOM)
#   MEM=4g ./run-stampede-docker.sh     # cap container RAM to provoke OOM-kill
#
# Env knobs forwarded to the bench: PARTITIONS, CHUNKS, CACHE_SECS, WITH_MINMAX,
# CONCURRENCY (e.g. CONCURRENCY=1,4,8,16,32).
set -euo pipefail

IMAGE="${IMAGE:-cubejs/rust-builder:bookworm-llvm-18}"
REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
MEM="${MEM:-}"                       # e.g. 4g — omit for no limit
PARTITIONS="${PARTITIONS:-2000}"
CHUNKS="${CHUNKS:-1500000}"          # ~1.5M chunks ≈ prod-38957 scale
CACHE_SECS="${CACHE_SECS:-0}"
WITH_MINMAX="${WITH_MINMAX:-1}"
CONCURRENCY="${CONCURRENCY:-1,2,4,8,16}"

mem_args=()
if [[ -n "$MEM" ]]; then
  # --oom-kill-disable=false (default): the kernel OOM-kills the bench when it
  # exceeds the cgroup limit, mirroring the production main-pod OOMKilled events.
  mem_args=(--memory "$MEM" --memory-swap "$MEM")
fi

exec docker run --rm -t \
  "${mem_args[@]}" \
  -v "$REPO_ROOT":/work -w /work \
  -e PARTITIONS -e CHUNKS -e CACHE_SECS -e WITH_MINMAX -e CONCURRENCY \
  -e CARGO_TARGET_DIR=/work/target-docker \
  "$IMAGE" \
  bash -lc "cargo bench -p cubestore --bench disk_space_stampede 2>&1 | tail -40"
