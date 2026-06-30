#!/usr/bin/env python3
"""
Update MDX frontmatter for Mintlify compatibility.

Transforms:
  - Removes `asIndexPage: true` (not needed in Mintlify)
  - Converts `toc: false` to `hideTableOfContents: true`
  - Adds `title` from the first H1 heading if not present
  - Adds `description` from first paragraph if not present
  - Removes `redirectFrom` (handled by mint.json redirects)

Usage:
    python update_frontmatter.py path/to/file.mdx
    python update_frontmatter.py path/to/docs/
    python update_frontmatter.py --dry-run path/to/docs/
    cat file.mdx | python update_frontmatter.py --stdin
"""

import argparse
import re
import sys
from pathlib import Path


def extract_h1(content_body: str) -> str:
    """Extract the first H1 heading from MDX content."""
    match = re.search(r"^#\s+(.+)$", content_body, re.MULTILINE)
    return match.group(1).strip() if match else ""


def extract_first_paragraph(content_body: str) -> str:
    """Extract the first non-empty paragraph after the H1."""
    # Skip the H1 line, find the next non-empty text block
    lines = content_body.split("\n")
    in_paragraph = False
    paragraph_lines = []

    for line in lines:
        stripped = line.strip()
        # Skip headings, empty lines before paragraph, and components
        if stripped.startswith("#") or stripped.startswith("<"):
            if in_paragraph:
                break
            continue
        if not stripped:
            if in_paragraph:
                break
            continue

        in_paragraph = True
        paragraph_lines.append(stripped)

    paragraph = " ".join(paragraph_lines)

    # Truncate to ~160 chars for SEO
    if len(paragraph) > 160:
        paragraph = paragraph[:157].rsplit(" ", 1)[0] + "..."

    # Remove markdown links but keep text
    paragraph = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", paragraph)
    # Remove inline code backticks
    paragraph = paragraph.replace("`", "")
    # Remove bold/italic
    paragraph = re.sub(r"\*+([^*]+)\*+", r"\1", paragraph)

    return paragraph


def parse_frontmatter(content: str):
    """
    Parse YAML frontmatter from MDX content.

    Returns (frontmatter_dict, frontmatter_raw, body).
    If no frontmatter, returns (None, "", content).
    """
    if not content.startswith("---"):
        return None, "", content

    end_match = re.search(r"\n---\s*\n", content[3:])
    if not end_match:
        return None, "", content

    fm_end = end_match.start() + 3  # offset from content[3:]
    fm_raw = content[3:fm_end + 3].strip()
    body = content[fm_end + 3 + 4:]  # skip past closing ---\n

    # Simple YAML parsing (key: value pairs)
    fm_dict = {}
    for line in fm_raw.split("\n"):
        line = line.strip()
        if ":" in line:
            key, _, value = line.partition(":")
            fm_dict[key.strip()] = value.strip()

    return fm_dict, fm_raw, body


def build_frontmatter(fm_dict: dict) -> str:
    """Build YAML frontmatter string from dict."""
    lines = ["---"]
    for key, value in fm_dict.items():
        # Quote string values that contain special chars
        if isinstance(value, str) and value and value != "true" and value != "false":
            # Already quoted
            if value.startswith('"') and value.endswith('"'):
                lines.append(f"{key}: {value}")
            elif value.startswith("'") and value.endswith("'"):
                lines.append(f"{key}: {value}")
            # Needs quoting if contains special YAML chars
            elif any(c in value for c in ":#{}[]|>&*!%@`"):
                escaped = value.replace('"', '\\"')
                lines.append(f'{key}: "{escaped}"')
            else:
                lines.append(f"{key}: {value}")
        else:
            lines.append(f"{key}: {value}")
    lines.append("---")
    return "\n".join(lines)


def update_frontmatter(content: str) -> str:
    """Update frontmatter for Mintlify compatibility."""
    fm_dict, fm_raw, body = parse_frontmatter(content)

    if fm_dict is None:
        fm_dict = {}

    # Remove Nextra-specific fields
    fm_dict.pop("asIndexPage", None)
    fm_dict.pop("redirectFrom", None)

    # Convert toc: false -> hideTableOfContents: true
    if fm_dict.get("toc") == "false":
        fm_dict.pop("toc")
        fm_dict["hideTableOfContents"] = "true"

    # Add title from H1 if missing
    if "title" not in fm_dict:
        h1 = extract_h1(body)
        if h1:
            fm_dict["title"] = h1

    # Add description if missing
    if "description" not in fm_dict:
        desc = extract_first_paragraph(body)
        if desc:
            fm_dict["description"] = desc

    # Reorder: title first, then description, then rest
    ordered = {}
    if "title" in fm_dict:
        ordered["title"] = fm_dict.pop("title")
    if "description" in fm_dict:
        ordered["description"] = fm_dict.pop("description")
    if "sidebarTitle" in fm_dict:
        ordered["sidebarTitle"] = fm_dict.pop("sidebarTitle")
    ordered.update(fm_dict)

    new_fm = build_frontmatter(ordered)
    return new_fm + "\n\n" + body.lstrip("\n")


def process_file(filepath: Path, dry_run: bool = False) -> bool:
    """Process a single file. Returns True if changes were made."""
    content = filepath.read_text(encoding="utf-8")
    transformed = update_frontmatter(content)

    if content == transformed:
        return False

    if dry_run:
        # Show the new frontmatter
        fm_end = transformed.index("\n---\n", 4)
        new_fm = transformed[:fm_end + 4]
        print(f"[WOULD CHANGE] {filepath}")
        print(f"  New frontmatter:")
        for line in new_fm.splitlines():
            print(f"    {line}")
        print()
    else:
        filepath.write_text(transformed, encoding="utf-8")
        print(f"[UPDATED] {filepath}")

    return True


def main():
    parser = argparse.ArgumentParser(
        description="Update MDX frontmatter for Mintlify compatibility."
    )
    parser.add_argument("path", nargs="?", help="File or directory to process")
    parser.add_argument("--dry-run", action="store_true", help="Show changes without writing")
    parser.add_argument("--stdin", action="store_true", help="Read from stdin, write to stdout")

    args = parser.parse_args()

    if args.stdin:
        content = sys.stdin.read()
        sys.stdout.write(update_frontmatter(content))
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
