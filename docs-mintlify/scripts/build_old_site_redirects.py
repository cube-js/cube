#!/usr/bin/env python3
"""
Build the cross-domain redirect table for the OLD docs site (Next.js/Nextra,
served at cube.dev/docs) so that every legacy URL 301s to the NEW Mintlify
docs at docs.cube.dev.

The old site already performs server-side redirects via `next.config.mjs` ->
`redirects()`. This script emits a JSON array of Next.js redirect objects with
ABSOLUTE destinations (https://docs.cube.dev/...), which `next.config.mjs` reads
and appends to its `redirects()` return value.

`basePath: false` is intentionally omitted. Next.js prefixes a redirect's
`source` with the configured `basePath` (e.g. /docs) only when `basePath` is not
set to false, while absolute (http/https) destinations are treated as external
and never get the basePath prepended. Omitting it therefore matches legacy URLs
under /docs and keeps the cross-domain destination intact.

Two layers are produced, in first-match-wins order:

  1. Specific page redirects — every entry from the old `redirects.json`, with
     its destination rewritten to the new path structure (PATH_REWRITES). These
     come first so consolidated/renamed pages reach their exact new home.

  2. Wildcard prefix redirects — one `/old/prefix/:path*` -> `/new/prefix/:path*`
     per PATH_REWRITES entry, plus a final `/product/:path*` -> `/docs/:path*`
     catch-all. These cover the bulk of pages that never needed an explicit
     redirect before. Next.js supports true wildcards, so subpaths are handled.

Usage:
    python build_old_site_redirects.py \
        --redirects ../docs/redirects.json \
        -o ../docs/redirects-new-docs.json
"""

import argparse
import json
import sys
from pathlib import Path

from rewrite_links import rewrite_path, PATH_REWRITES

NEW_DOCS_ORIGIN = "https://docs.cube.dev"


def to_absolute(path: str) -> str:
    """Turn a root-relative new-site path into an absolute new-docs URL."""
    if path.startswith(("http://", "https://")):
        return path
    return NEW_DOCS_ORIGIN + path


def build_specific_redirects(old_redirects: list[dict]) -> list[dict]:
    """Rewrite each legacy redirect's destination and make it cross-domain."""
    out = []
    for r in old_redirects:
        source = r.get("source", "")
        destination = rewrite_path(r.get("destination", ""))
        out.append({
            "source": source,
            "destination": to_absolute(destination),
            "permanent": True,
        })
    return out


def build_wildcard_redirects() -> list[dict]:
    """One wildcard per PATH_REWRITES prefix, plus a /product catch-all.

    PATH_REWRITES is already ordered most-specific-first, which is the order
    Next.js needs for first-match-wins among the wildcards.
    """
    out = []
    for old_prefix, new_prefix in PATH_REWRITES:
        # Exact prefix (e.g. /product/configuration itself).
        out.append({
            "source": old_prefix,
            "destination": to_absolute(new_prefix),
            "permanent": True,
        })
        # All subpaths under the prefix.
        out.append({
            "source": f"{old_prefix}/:path*",
            "destination": to_absolute(f"{new_prefix}/:path*"),
            "permanent": True,
        })
    # Final catch-all: any remaining /product/* mirrors rewrite_path's default.
    out.append({
        "source": "/product",
        "destination": to_absolute("/docs"),
        "permanent": True,
    })
    out.append({
        "source": "/product/:path*",
        "destination": to_absolute("/docs/:path*"),
        "permanent": True,
    })
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--redirects",
        default="../docs/redirects.json",
        help="Path to the old site's redirects.json (default: ../docs/redirects.json)",
    )
    parser.add_argument(
        "-o", "--output",
        default="../docs/redirects-new-docs.json",
        help="Output path (default: ../docs/redirects-new-docs.json)",
    )
    args = parser.parse_args()

    redirects_path = Path(args.redirects)
    if not redirects_path.exists():
        print(f"Error: {redirects_path} not found", file=sys.stderr)
        sys.exit(1)

    old_redirects = json.loads(redirects_path.read_text(encoding="utf-8"))
    specific = build_specific_redirects(old_redirects)
    wildcard = build_wildcard_redirects()

    # Specifics first (exact pages win), wildcards last (catch-all fallbacks).
    # Drop later duplicate sources so a specific page redirect always wins over
    # the wildcard prefix's exact-match entry for the same source.
    combined = []
    seen = set()
    for r in specific + wildcard:
        if r["source"] in seen:
            continue
        seen.add(r["source"])
        combined.append(r)

    Path(args.output).write_text(
        json.dumps(combined, indent=2) + "\n", encoding="utf-8"
    )
    print(
        f"Wrote {len(combined)} redirects "
        f"({len(specific)} specific + {len(wildcard)} wildcard) to {args.output}",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
