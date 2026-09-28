#!/bin/sh
# Cube CLI installer for Linux and macOS.
#
#   curl -fsSL https://raw.githubusercontent.com/cube-js/cube/master/install-cli.sh | sh
#
# Environment overrides:
#   CUBE_INSTALL_DIR      install directory (default: /usr/local/bin if
#                         writable, else ~/.local/bin)
#   CUBE_VERSION          release tag to install, e.g. v1.7.5 (default: latest)
set -eu

REPO="cube-js/cube"

main() {
    os=$(uname -s)
    arch=$(uname -m)
    case "$os" in
        Linux)  os_part="unknown-linux-musl" ;;
        Darwin) os_part="apple-darwin" ;;
        *) err "unsupported OS: $os (use install.ps1 on Windows)" ;;
    esac
    case "$arch" in
        x86_64|amd64)  arch_part="x86_64" ;;
        arm64|aarch64) arch_part="aarch64" ;;
        *) err "unsupported architecture: $arch" ;;
    esac
    target="${arch_part}-${os_part}"

    if [ "${CUBE_VERSION:-latest}" = "latest" ]; then
        url="https://github.com/${REPO}/releases/latest/download/cube-${target}.tar.gz"
    else
        url="https://github.com/${REPO}/releases/download/${CUBE_VERSION}/cube-${target}.tar.gz"
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

    echo "Downloading cube (${target}) from ${url}…"
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL "$url" -o "$tmp/cube.tar.gz"
    elif command -v wget >/dev/null 2>&1; then
        wget -qO "$tmp/cube.tar.gz" "$url"
    else
        err "neither curl nor wget is available"
    fi

    tar -xzf "$tmp/cube.tar.gz" -C "$tmp"
    install -m 755 "$tmp/cube" "$dir/cube"

    echo "Installed $("$dir/cube" --version) to $dir/cube"
    case ":$PATH:" in
        *":$dir:"*) ;;
        *) echo "NOTE: $dir is not on your PATH — add it, e.g.:"
           echo "  export PATH=\"$dir:\$PATH\"" ;;
    esac
}

err() {
    echo "error: $1" >&2
    exit 1
}

main "$@"
