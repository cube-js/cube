#!/usr/bin/env python3
"""
Copy and restructure content from Nextra docs to Mintlify folder layout.

Copies MDX files from docs/content/product/ to the new tab-based structure
in docs-mintlify/, preserving directory hierarchy and renaming as needed.

Usage:
    # Full migration (copies all content):
    python copy_content.py --source ../docs/content/product --dest ..

    # Migrate a single section:
    python copy_content.py --source ../docs/content/product --dest .. --section getting-started

    # Migrate late-arriving content (only new/changed files):
    python copy_content.py --source ../docs/content/product --dest .. --incremental

    # Dry run:
    python copy_content.py --source ../docs/content/product --dest .. --dry-run
"""

import argparse
import hashlib
import shutil
import sys
from pathlib import Path


# Section -> (destination tab folder, destination subfolder)
# If subfolder is None, files go directly into the tab folder
SECTION_MAP = {
    # Docs tab
    "introduction.mdx": ("docs", None),
    "getting-started": ("docs", "getting-started"),
    "configuration": ("docs", "configuration"),
    "data-modeling": ("docs", "data-modeling"),
    "caching": ("docs", "caching"),

    # Analytics tab (merged exploration + presentation)
    "exploration": ("analytics", None),
    "presentation": ("analytics", None),

    # Embedding tab
    "embedding": ("embedding", None),

    # Access & Security tab (from auth/ + parts of administration/)
    "auth": ("access-security", None),

    # Administration tab (most of administration/)
    "administration": ("admin", None),

    # APIs & Integrations -> split across tabs
    "apis-integrations": ("_split", None),  # Special handling
}

# Within apis-integrations, these go to api-reference/
API_REFERENCE_DIRS = {
    "core-data-apis",
    "embed-apis",
    "control-plane-api.mdx",
    "orchestration-api",
    "javascript-sdk",
    "mcp-server.mdx",
    "recipes",
}

# Within apis-integrations, these go to docs/integrations/
INTEGRATION_FILES = {
    "microsoft-excel.mdx",
    "google-sheets.mdx",
    "tableau.mdx",
    "power-bi.mdx",
    "semantic-layer-sync",
    "snowflake-semantic-views.mdx",
}

# Within administration/, these go to access-security/ instead of admin/
ACCESS_SECURITY_DIRS = {
    "sso",
    "users-and-permissions",
}


def file_hash(filepath: Path) -> str:
    """Compute MD5 hash of a file for incremental comparison."""
    return hashlib.md5(filepath.read_bytes()).hexdigest()


def should_copy(src: Path, dest: Path, incremental: bool) -> bool:
    """Determine if a file should be copied."""
    if not incremental:
        return True
    if not dest.exists():
        return True
    return file_hash(src) != file_hash(dest)


def copy_file(src: Path, dest: Path, dry_run: bool = False) -> bool:
    """Copy a single file, creating parent dirs as needed."""
    if dry_run:
        print(f"  [WOULD COPY] {src.name} -> {dest}")
        return True

    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dest)
    return True


def copy_directory(src_dir: Path, dest_dir: Path, dry_run: bool, incremental: bool) -> int:
    """Recursively copy a directory of MDX files. Returns count of files copied."""
    count = 0
    for src_file in sorted(src_dir.rglob("*.mdx")):
        relative = src_file.relative_to(src_dir)
        dest_file = dest_dir / relative

        if should_copy(src_file, dest_file, incremental):
            copy_file(src_file, dest_file, dry_run)
            count += 1

    return count


def migrate_apis_integrations(
    src_dir: Path, dest_root: Path, dry_run: bool, incremental: bool
) -> int:
    """Split apis-integrations into api-reference/ and docs/integrations/."""
    count = 0

    for item in sorted(src_dir.iterdir()):
        name = item.name

        if name == "_meta.js":
            continue

        if name == "index.mdx":
            # The index page goes to api-reference/
            dest = dest_root / "api-reference" / "index.mdx"
            if should_copy(item, dest, incremental):
                copy_file(item, dest, dry_run)
                count += 1
            continue

        if name in API_REFERENCE_DIRS:
            if item.is_dir():
                count += copy_directory(
                    item,
                    dest_root / "api-reference" / name,
                    dry_run,
                    incremental,
                )
            else:
                dest = dest_root / "api-reference" / name
                if should_copy(item, dest, incremental):
                    copy_file(item, dest, dry_run)
                    count += 1

        elif name in INTEGRATION_FILES:
            if item.is_dir():
                count += copy_directory(
                    item,
                    dest_root / "docs" / "integrations" / name,
                    dry_run,
                    incremental,
                )
            else:
                dest = dest_root / "docs" / "integrations" / name
                if should_copy(item, dest, incremental):
                    copy_file(item, dest, dry_run)
                    count += 1

        else:
            # Default: goes to api-reference/
            if item.is_dir():
                count += copy_directory(
                    item,
                    dest_root / "api-reference" / name,
                    dry_run,
                    incremental,
                )
            elif item.suffix == ".mdx":
                dest = dest_root / "api-reference" / name
                if should_copy(item, dest, incremental):
                    copy_file(item, dest, dry_run)
                    count += 1

    return count


def migrate_administration(
    src_dir: Path, dest_root: Path, dry_run: bool, incremental: bool
) -> int:
    """Split administration/ into admin/ and access-security/."""
    count = 0

    for item in sorted(src_dir.iterdir()):
        name = item.name

        if name == "_meta.js":
            continue

        if name in ACCESS_SECURITY_DIRS:
            if item.is_dir():
                count += copy_directory(
                    item,
                    dest_root / "access-security" / name,
                    dry_run,
                    incremental,
                )
            continue

        # Everything else goes to admin/
        if item.is_dir():
            count += copy_directory(
                item,
                dest_root / "admin" / name,
                dry_run,
                incremental,
            )
        elif item.suffix == ".mdx":
            dest = dest_root / "admin" / name
            if should_copy(item, dest, incremental):
                copy_file(item, dest, dry_run)
                count += 1

    return count


def migrate_auth(
    src_dir: Path, dest_root: Path, dry_run: bool, incremental: bool
) -> int:
    """Copy auth/ to access-security/access-control/ with methods/ -> authentication/."""
    count = 0

    for item in sorted(src_dir.iterdir()):
        name = item.name

        if name == "_meta.js":
            continue

        if name == "methods":
            # auth/methods/ -> access-security/authentication/
            if item.is_dir():
                count += copy_directory(
                    item,
                    dest_root / "access-security" / "authentication",
                    dry_run,
                    incremental,
                )
        elif item.is_dir():
            count += copy_directory(
                item,
                dest_root / "access-security" / "access-control" / name,
                dry_run,
                incremental,
            )
        elif item.suffix == ".mdx":
            dest = dest_root / "access-security" / "access-control" / name
            if should_copy(item, dest, incremental):
                copy_file(item, dest, dry_run)
                count += 1

    return count


def migrate_section(
    section_name: str,
    src_root: Path,
    dest_root: Path,
    dry_run: bool,
    incremental: bool,
) -> int:
    """Migrate a single section. Returns count of files copied."""
    src_path = src_root / section_name

    # Special cases
    if section_name == "apis-integrations":
        return migrate_apis_integrations(src_path, dest_root, dry_run, incremental)

    if section_name == "administration":
        return migrate_administration(src_path, dest_root, dry_run, incremental)

    if section_name == "auth":
        return migrate_auth(src_path, dest_root, dry_run, incremental)

    # Standard section mapping
    mapping = SECTION_MAP.get(section_name)
    if not mapping:
        print(f"  [SKIP] Unknown section: {section_name}", file=sys.stderr)
        return 0

    tab_folder, subfolder = mapping

    if subfolder:
        dest_dir = dest_root / tab_folder / subfolder
    else:
        dest_dir = dest_root / tab_folder

    # Handle single file (e.g., introduction.mdx)
    if section_name.endswith(".mdx"):
        src_file = src_root / section_name
        if src_file.exists():
            dest_file = dest_root / tab_folder / section_name
            if should_copy(src_file, dest_file, incremental):
                copy_file(src_file, dest_file, dry_run)
                return 1
        return 0

    if not src_path.is_dir():
        print(f"  [SKIP] Source not found: {src_path}", file=sys.stderr)
        return 0

    return copy_directory(src_path, dest_dir, dry_run, incremental)


def main():
    parser = argparse.ArgumentParser(
        description="Copy and restructure Nextra docs to Mintlify folder layout."
    )
    parser.add_argument(
        "--source",
        required=True,
        help="Source directory (e.g., ../docs/content/product)",
    )
    parser.add_argument(
        "--dest",
        required=True,
        help="Destination root (e.g., .. for docs-mintlify/)",
    )
    parser.add_argument(
        "--section",
        help="Migrate only a specific section (e.g., getting-started, configuration)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be copied without copying",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Only copy new or changed files (compares by hash)",
    )

    args = parser.parse_args()

    src_root = Path(args.source).resolve()
    dest_root = Path(args.dest).resolve()

    if not src_root.is_dir():
        print(f"Error: Source directory not found: {src_root}", file=sys.stderr)
        sys.exit(1)

    total_copied = 0

    if args.section:
        sections = [args.section]
    else:
        # All sections in order
        sections = [
            "introduction.mdx",
            "getting-started",
            "configuration",
            "data-modeling",
            "caching",
            "exploration",
            "presentation",
            "embedding",
            "auth",
            "administration",
            "apis-integrations",
        ]

    for section in sections:
        print(f"\nMigrating: {section}")
        count = migrate_section(section, src_root, dest_root, args.dry_run, args.incremental)
        total_copied += count
        print(f"  {count} files {'would be ' if args.dry_run else ''}copied")

    print(f"\nTotal: {total_copied} files {'would be ' if args.dry_run else ''}copied")


if __name__ == "__main__":
    main()
