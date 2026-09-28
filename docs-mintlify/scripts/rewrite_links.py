#!/usr/bin/env python3
"""
Rewrite internal links from Nextra /product/ paths to Mintlify tab-based paths.

Handles:
  - Inline links: [text](/product/configuration/...)
  - Reference-style definitions: [ref-name]: /product/configuration/...
  - Relative links within the same section

Usage:
    # Rewrite a single file (in-place):
    python rewrite_links.py path/to/file.mdx

    # Rewrite a directory recursively:
    python rewrite_links.py path/to/docs/

    # Dry run:
    python rewrite_links.py --dry-run path/to/docs/

    # Pipe mode:
    cat file.mdx | python rewrite_links.py --stdin
"""

import argparse
import re
import sys
from pathlib import Path


# Maps old /product/... paths to new tab-based paths.
# Order matters: more specific prefixes must come first.
PATH_REWRITES = [
    # APIs & Integrations -> split between api-reference/ and docs/integrations/
    # API reference pages
    ("/product/apis-integrations/core-data-apis", "/api-reference/core-data-apis"),
    ("/product/apis-integrations/embed-apis", "/api-reference/embed-apis"),
    ("/product/apis-integrations/control-plane-api", "/api-reference/control-plane-api"),
    ("/product/apis-integrations/orchestration-api", "/api-reference/orchestration-api"),
    ("/product/apis-integrations/javascript-sdk", "/api-reference/javascript-sdk"),
    ("/product/apis-integrations/mcp-server", "/api-reference/mcp-server"),
    ("/product/apis-integrations/recipes", "/api-reference/recipes"),
    # Integration pages -> stay in docs
    ("/product/apis-integrations/microsoft-excel", "/docs/integrations/microsoft-excel"),
    ("/product/apis-integrations/google-sheets", "/docs/integrations/google-sheets"),
    ("/product/apis-integrations/tableau", "/docs/integrations/tableau"),
    ("/product/apis-integrations/power-bi", "/docs/integrations/power-bi"),
    ("/product/apis-integrations/semantic-layer-sync", "/docs/integrations/semantic-layer-sync"),
    ("/product/apis-integrations/snowflake-semantic-views", "/docs/integrations/snowflake-semantic-views"),
    # Catch-all for any remaining apis-integrations
    ("/product/apis-integrations", "/api-reference"),

    # Access control -> access-security/
    ("/product/auth/methods", "/access-security/authentication"),
    ("/product/auth", "/access-security/access-control"),

    # Administration -> split between admin/ and access-security/
    ("/product/administration/sso", "/access-security/sso"),
    ("/product/administration/users-and-permissions", "/access-security/users-and-permissions"),
    ("/product/administration", "/admin"),

    # Exploration -> analytics/
    ("/product/exploration", "/analytics"),

    # Presentation -> analytics/
    ("/product/presentation", "/analytics"),

    # Embedding -> embedding/
    ("/product/embedding", "/embedding"),

    # Core docs sections -> docs/
    ("/product/getting-started", "/docs/getting-started"),
    ("/product/configuration", "/docs/configuration"),
    ("/product/data-modeling", "/docs/data-modeling"),
    ("/product/caching", "/docs/caching"),
    ("/product/introduction", "/docs/introduction"),
]


def rewrite_path(old_path: str) -> str:
    """Rewrite a single path from Nextra to Mintlify structure."""
    for old_prefix, new_prefix in PATH_REWRITES:
        if old_path == old_prefix or old_path.startswith(old_prefix + "/") or old_path.startswith(old_prefix + "#"):
            # Preserve the rest of the path after the prefix
            remainder = old_path[len(old_prefix):]
            return new_prefix + remainder

    # If no rewrite rule matches but starts with /product/, strip it
    if old_path.startswith("/product/"):
        return "/docs/" + old_path[len("/product/"):]

    return old_path


def rewrite_links_in_content(content: str) -> str:
    """Rewrite all internal links in MDX content."""

    # 1. Inline markdown links: [text](/product/...)
    def inline_link_replacer(match):
        text = match.group(1)
        path = match.group(2)
        if path.startswith("/product/"):
            new_path = rewrite_path(path)
            return f"[{text}]({new_path})"
        return match.group(0)

    content = re.sub(
        r"\[([^\]]*)\]\((/product/[^)]+)\)",
        inline_link_replacer,
        content,
    )

    # 2. Reference-style link definitions: [ref-name]: /product/...
    def reflink_replacer(match):
        ref_name = match.group(1)
        path = match.group(2)
        if path.startswith("/product/"):
            new_path = rewrite_path(path)
            return f"[{ref_name}]: {new_path}"
        return match.group(0)

    content = re.sub(
        r"^\[([^\]]+)\]:\s*(/product/[^\s]+)\s*$",
        reflink_replacer,
        content,
        flags=re.MULTILINE,
    )

    # 3. href attributes in JSX: href="/product/..."
    def href_replacer(match):
        path = match.group(1)
        if path.startswith("/product/"):
            new_path = rewrite_path(path)
            return f'href="{new_path}"'
        return match.group(0)

    content = re.sub(
        r'href="(/product/[^"]+)"',
        href_replacer,
        content,
    )

    # 4. url attributes in JSX (used in GridItem): url="/product/..."
    def url_replacer(match):
        path = match.group(1)
        if path.startswith("/product/"):
            new_path = rewrite_path(path)
            return f'url="{new_path}"'
        return match.group(0)

    content = re.sub(
        r'url="(/product/[^"]+)"',
        url_replacer,
        content,
    )

    return content


def process_file(filepath: Path, dry_run: bool = False) -> bool:
    """Process a single file. Returns True if changes were made."""
    content = filepath.read_text(encoding="utf-8")
    transformed = rewrite_links_in_content(content)

    if content == transformed:
        return False

    if dry_run:
        print(f"[WOULD CHANGE] {filepath}")
        orig_lines = content.splitlines()
        new_lines = transformed.splitlines()
        changes = 0
        for i, (old, new) in enumerate(zip(orig_lines, new_lines)):
            if old != new:
                changes += 1
                if changes <= 15:
                    print(f"  L{i+1}:")
                    print(f"    - {old.strip()}")
                    print(f"    + {new.strip()}")
        if changes > 15:
            print(f"  ... and {changes - 15} more changes")
        print()
    else:
        filepath.write_text(transformed, encoding="utf-8")
        print(f"[UPDATED] {filepath}")

    return True


def main():
    parser = argparse.ArgumentParser(
        description="Rewrite internal links from Nextra /product/ paths to Mintlify tab-based paths."
    )
    parser.add_argument("path", nargs="?", help="File or directory to process")
    parser.add_argument("--dry-run", action="store_true", help="Show changes without writing")
    parser.add_argument("--stdin", action="store_true", help="Read from stdin, write to stdout")

    args = parser.parse_args()

    if args.stdin:
        content = sys.stdin.read()
        sys.stdout.write(rewrite_links_in_content(content))
        return

    if not args.path:
        parser.error("Either --stdin or a path is required")

    target = Path(args.path)

    if target.is_file():
        changed = process_file(target, dry_run=args.dry_run)
        if not changed:
            print(f"[NO CHANGES] {target}")
    elif target.is_dir():
        changed_count = 0
        total_count = 0
        for mdx_file in sorted(target.rglob("*.mdx")):
            total_count += 1
            if process_file(mdx_file, dry_run=args.dry_run):
                changed_count += 1
        print(f"\nProcessed {total_count} files, {changed_count} changed.")
    else:
        print(f"Error: {target} not found", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
