#!/usr/bin/env bash
# Runs install-cli.sh (and install-cli.ps1 when pwsh is available) against a
# local fake of the GitHub releases API and checks which release they install.
#
#   rust/cube-cli/tests/installers.sh
set -euo pipefail

root=$(cd "$(dirname "$0")/../../.." && pwd)
work=$(mktemp -d)
server_pid=""
cleanup() {
    if [ -n "$server_pid" ]; then kill "$server_pid" 2>/dev/null || true; fi
    rm -rf "$work"
}
trap cleanup EXIT

port=$((20000 + RANDOM % 20000))
base="http://127.0.0.1:${port}"
targets="x86_64-unknown-linux-musl aarch64-unknown-linux-musl x86_64-apple-darwin aarch64-apple-darwin x86_64-pc-windows-msvc"

# Serves fake CLI archives for a release: a script that reports the version,
# under both binary names the installers look for.
publish() {
    local tag=$1 bin="$work/bin-$1"
    mkdir -p "$bin" "$work/srv/download/$tag"
    printf '#!/bin/sh\necho "Cube CLI %s"\n' "${tag#v}" > "$bin/cube"
    chmod 755 "$bin/cube"
    cp "$bin/cube" "$bin/cube.exe"
    for t in $targets; do
        tar -C "$bin" -czf "$work/srv/download/$tag/cube-$t.tar.gz" cube cube.exe
    done
}

# A release object as the GitHub API returns it. $3 is "all" to list a CLI
# asset for every target, anything else to list none; $4 marks a draft.
release() {
    local tag=$1 prerelease=$2 assets=$3 draft=${4:-false}
    printf '{"tag_name":"%s","name":"%s","draft":%s,"prerelease":%s,"assets":[' "$tag" "$tag" "$draft" "$prerelease"
    # A non-CLI asset, like the cubestored and native ones on real releases.
    printf '{"name":"cubestored-x86_64.tar.gz","state":"uploaded","browser_download_url":"%s/download/%s/cubestored-x86_64.tar.gz"}' "$base" "$tag"
    if [ "$assets" = all ]; then
        for t in $targets; do
            printf ',{"name":"cube-%s.tar.gz","state":"uploaded","browser_download_url":"%s/download/%s/cube-%s.tar.gz"}' "$t" "$base" "$tag" "$t"
        done
    fi
    # Release notes can quote JSON; it must not be mistaken for release fields.
    printf '],"body":"Run \\"cube\\", see {\\"tag_name\\": \\"v9.9.9\\", \\"prerelease\\": false}"}'
}

# Listed newest-published first, like the API:
#   v2.2.0       a draft, listed to tokens with push access; never installed
#   v2.1.0       newest, but its CLI uploads failed: nothing to install
#   v1.6.99      a maintenance release published after v2.0.1, lower version
#   v3.0.0-beta  a prerelease, never picked by default
#   v2.0.1       lists the assets, but downloading them fails
#   v2.0.0       the one to install
#   v1.9.0       older
mkdir -p "$work/srv"
publish v2.2.0
publish v1.6.99
publish v3.0.0-beta
publish v2.0.0
publish v1.9.0
{
    printf '['
    release v2.2.0 false all true; printf ','
    release v2.1.0 false none; printf ','
    release v1.6.99 false all; printf ','
    release v3.0.0-beta true all; printf ','
    release v2.0.1 false all; printf ','
    release v2.0.0 false all; printf ','
    release v1.9.0 false all
    printf ']'
} > "$work/srv/releases.json"

python3 -m http.server "$port" --bind 127.0.0.1 --directory "$work/srv" >/dev/null 2>&1 &
server_pid=$!
for _ in $(seq 50); do
    if curl -fs "$base/releases.json" -o /dev/null; then break; fi
    sleep 0.1
done

failed=0
check() {
    local name=$1 bin=$2 want=$3 got
    got=$("$bin" --version 2>&1 || true)
    if [ "$got" = "$want" ]; then
        echo "ok: $name installed '$got'"
    else
        echo "FAIL: $name installed '$got', expected '$want'" >&2
        failed=1
    fi
}

export CUBE_RELEASES_URL="$base/releases.json"

CUBE_INSTALL_DIR="$work/sh" sh "$root/install-cli.sh" || true
check install-cli.sh "$work/sh/cube" "Cube CLI 2.0.0"

if command -v pwsh >/dev/null 2>&1; then
    CUBE_INSTALL_DIR="$work/ps1" pwsh -NoProfile -NonInteractive -File "$root/install-cli.ps1" || true
    check install-cli.ps1 "$work/ps1/cube.exe" "Cube CLI 2.0.0"
else
    echo "skip: install-cli.ps1 (pwsh not found)"
fi

exit "$failed"
