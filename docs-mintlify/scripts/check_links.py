#!/usr/bin/env python3
"""
Verify all internal links in Mintlify docs resolve to existing files.

Checks:
  - Inline links: [text](/docs/something)
  - Reference-style links: [ref]: /docs/something
  - href/url attributes: href="/docs/something"

Usage:
    python check_links.py /path/to/docs-mintlify/
    python check_links.py /path/to/docs-mintlify/ --verbose
"""

import argparse
import re
import sys
from pathlib import Path


def extract_internal_links(content: str, filepath: Path) -> list[tuple[int, str]]:
    """Extract all internal links from MDX content. Returns [(line_number, path), ...]."""
    links = []

    for i, line in enumerate(content.splitlines(), 1):
        # Inline links: [text](/path)
        for match in re.finditer(r"\[([^\]]*)\]\((/[^)#\s]+)", line):
            links.append((i, match.group(2)))

        # Reference-style definitions: [ref]: /path
        ref_match = re.match(r"^\[([^\]]+)\]:\s*(/[^\s#]+)", line)
        if ref_match:
            links.append((i, ref_match.group(2)))

        # href attributes: href="/path"
        for match in re.finditer(r'href="(/[^"#]+)"', line):
            links.append((i, match.group(1)))

        # url attributes: url="/path"
        for match in re.finditer(r'url="(/[^"#]+)"', line):
            path = match.group(1)
            # Skip external-looking URLs
            if not path.startswith("http"):
                links.append((i, path))

    return links


def resolve_link(link_path: str, docs_root: Path) -> bool:
    """Check if an internal link resolves to an existing file."""
    # Remove leading slash
    relative = link_path.lstrip("/")

    # Try exact match with .mdx extension
    candidates = [
        docs_root / f"{relative}.mdx",
        docs_root / relative / "index.mdx",
        docs_root / relative,  # exact file (e.g., if it has extension)
    ]

    return any(c.exists() for c in candidates)


def check_directory(docs_root: Path, verbose: bool = False) -> tuple[int, int, list]:
    """Check all MDX files in directory. Returns (total_links, broken_count, broken_list)."""
    total_links = 0
    broken = []

    for mdx_file in sorted(docs_root.rglob("*.mdx")):
        # Skip scripts directory
        if "scripts" in mdx_file.parts:
            continue

        content = mdx_file.read_text(encoding="utf-8")
        links = extract_internal_links(content, mdx_file)
        total_links += len(links)

        for line_num, link_path in links:
            # Skip external links
            if link_path.startswith("http"):
                continue

            if not resolve_link(link_path, docs_root):
                relative_file = mdx_file.relative_to(docs_root)
                broken.append((str(relative_file), line_num, link_path))

                if verbose:
                    print(f"  BROKEN: {relative_file}:{line_num} -> {link_path}")

    return total_links, len(broken), broken


def main():
    parser = argparse.ArgumentParser(
        description="Check internal links in Mintlify docs."
    )
    parser.add_argument(
        "docs_root",
        help="Root directory of Mintlify docs",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show each broken link as it's found",
    )

    args = parser.parse_args()
    docs_root = Path(args.docs_root).resolve()

    if not docs_root.is_dir():
        print(f"Error: {docs_root} not found", file=sys.stderr)
        sys.exit(1)

    print(f"Checking links in {docs_root}...\n")

    total, broken_count, broken = check_directory(docs_root, verbose=args.verbose)

    print(f"\nResults:")
    print(f"  Total internal links: {total}")
    print(f"  Broken links: {broken_count}")

    if broken and not args.verbose:
        print(f"\nBroken links:")
        for filepath, line, link in broken:
            print(f"  {filepath}:{line} -> {link}")

    if broken_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
