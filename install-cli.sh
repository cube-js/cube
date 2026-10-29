#!/bin/sh
# Cube CLI installer for Linux and macOS.
#
#   curl -fsSL https://raw.githubusercontent.com/cube-js/cube/master/install-cli.sh | sh
#
# Environment overrides:
#   CUBE_INSTALL_DIR      install directory (default: /usr/local/bin if
#                         writable, else ~/.local/bin)
#   CUBE_VERSION          release tag to install, e.g. v1.7.5 (default: the
#                         newest release that ships a binary for this platform)
#   CUBE_RELEASES_URL     GitHub releases API endpoint to pick the version from
#                         (default: the cube-js/cube releases, used by tests)
#   GITHUB_TOKEN          sent to api.github.com only, to avoid its
#                         unauthenticated rate limit on shared CI runners
set -eu

REPO="cube-js/cube"
RELEASES_URL="${CUBE_RELEASES_URL:-https://api.github.com/repos/${REPO}/releases?per_page=30}"
# How many releases to try before giving up when downloads keep failing.
MAX_ATTEMPTS=3

main() {
    os=$(uname -s)
    arch=$(uname -m)
    case "$os" in
        Linux)  os_part="unknown-linux-musl" ;;
        Darwin) os_part="apple-darwin" ;;
        *) err "unsupported OS: $os (use install-cli.ps1 on Windows)" ;;
    esac
    case "$arch" in
        x86_64|amd64)  arch_part="x86_64" ;;
        arm64|aarch64) arch_part="aarch64" ;;
        *) err "unsupported architecture: $arch" ;;
    esac
    target="${arch_part}-${os_part}"
    asset="cube-${target}.tar.gz"

    if ! command -v curl >/dev/null 2>&1 && ! command -v wget >/dev/null 2>&1; then
        err "neither curl nor wget is available"
    fi

    dir="${CUBE_INSTALL_DIR:-}"
    if [ -z "$dir" ]; then
        if [ -w /usr/local/bin ]; then
            dir=/usr/local/bin
        else
            dir="$HOME/.local/bin"
        fi
    fi
    mkdir -p "$dir"

    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' EXIT

    if [ -n "${CUBE_VERSION:-}" ] && [ "$CUBE_VERSION" != "latest" ]; then
        echo "https://github.com/${REPO}/releases/download/${CUBE_VERSION}/${asset}" > "$tmp/urls"
    else
        # Not releases/latest: its binaries upload after publishing and can be
        # missing (v1.7.14).
        if fetch_api "$RELEASES_URL" "$tmp/releases.json" 2>/dev/null; then
            release_urls "$tmp/releases.json" "$asset" | head -n "$MAX_ATTEMPTS" > "$tmp/urls"
        fi
        if [ ! -s "$tmp/urls" ]; then
            echo "warning: could not list releases, falling back to the latest one" >&2
            echo "https://github.com/${REPO}/releases/latest/download/${asset}" > "$tmp/urls"
        fi
    fi

    downloaded=""
    while read -r url; do
        echo "Downloading cube (${target}) from ${url}…"
        if download "$url" "$tmp/cube.tar.gz"; then
            downloaded=1
            break
        fi
        echo "warning: download failed, trying an older release" >&2
    done < "$tmp/urls"
    [ -n "$downloaded" ] || err "could not download ${asset}"

    tar -xzf "$tmp/cube.tar.gz" -C "$tmp"
    install -m 755 "$tmp/cube" "$dir/cube"

    echo "Installed $("$dir/cube" --version) to $dir/cube"
    case ":$PATH:" in
        *":$dir:"*) ;;
        *) echo "NOTE: $dir is not on your PATH — add it, e.g.:"
           echo "  export PATH=\"$dir:\$PATH\"" ;;
    esac
}

# Prints the download URL of $2 for every stable release in the GitHub
# releases API response $1 that lists it, highest version first. Releases from
# older maintenance lines are interleaved by publish date, so order by version.
release_urls() {
    # tag_name, draft and prerelease precede the assets array in each release.
    # Drafts are only listed to a GITHUB_TOKEN with push access.
    grep -oE '"(tag_name|draft|prerelease|browser_download_url)": *("[^"]*"|true|false)' "$1" |
        awk -v asset="$2" '
            { v = $0; sub(/^"[a-z_]*": */, "", v); gsub(/"/, "", v) }
            /^"tag_name"/ { tag = v; draft = 1; stable = 0; next }
            /^"draft"/ { draft = (v == "true"); next }
            /^"prerelease"/ { stable = (v == "false") && !draft; next }
            stable && substr(v, length(v) - length(asset)) == "/" asset {
                ver = tag; sub(/^v/, "", ver); print ver, v
            }
        ' |
        sort -t. -k1,1nr -k2,2nr -k3,3nr |
        cut -d' ' -f2
}

fetch_api() {
    # Only GitHub gets the token, never a CUBE_RELEASES_URL mirror.
    token=""
    case "$1" in
        https://api.github.com/*) token="${GITHUB_TOKEN:-}" ;;
    esac
    if command -v curl >/dev/null 2>&1; then
        if [ -n "$token" ]; then
            curl -fsSL --compressed -H "Authorization: Bearer ${token}" "$1" -o "$2"
        else
            curl -fsSL --compressed "$1" -o "$2"
        fi
    elif [ -n "$token" ]; then
        wget -qO "$2" --header "Authorization: Bearer ${token}" "$1"
    else
        wget -qO "$2" "$1"
    fi
}

download() {
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL "$1" -o "$2"
    else
        wget -qO "$2" "$1"
    fi
}

err() {
    echo "error: $1" >&2
    exit 1
}

main "$@"
