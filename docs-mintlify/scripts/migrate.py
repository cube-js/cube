#!/usr/bin/env python3
"""
Main migration orchestrator. Runs all migration steps in sequence.

Usage:
    # Full migration (all steps):
    python migrate.py --source ../../docs/content/product --dest ../../docs-mintlify

    # Dry run (preview everything):
    python migrate.py --source ../../docs/content/product --dest ../../docs-mintlify --dry-run

    # Run specific steps only:
    python migrate.py --source ../../docs/content/product --dest ../../docs-mintlify --steps copy,transform,links

    # Incremental (for late-arriving content):
    python migrate.py --source ../../docs/content/product --dest ../../docs-mintlify --incremental

    # Migrate a single section:
    python migrate.py --source ../../docs/content/product --dest ../../docs-mintlify --section getting-started
"""

import argparse
import subprocess
import sys
from pathlib import Path


SCRIPTS_DIR = Path(__file__).parent.resolve()

STEPS = {
    "copy": {
        "description": "Copy and restructure content files",
        "script": "copy_content.py",
    },
    "transform": {
        "description": "Transform Nextra components to Mintlify",
        "script": "transform_components.py",
    },
    "frontmatter": {
        "description": "Update frontmatter for Mintlify",
        "script": "update_frontmatter.py",
    },
    "links": {
        "description": "Rewrite internal links",
        "script": "rewrite_links.py",
    },
    "check": {
        "description": "Verify all internal links",
        "script": "check_links.py",
    },
}

DEFAULT_STEPS = ["copy", "transform", "frontmatter", "links", "check"]


def run_step(
    step_name: str,
    source: Path,
    dest: Path,
    dry_run: bool,
    incremental: bool,
    section,
) -> bool:
    """Run a single migration step. Returns True on success."""
    step = STEPS[step_name]
    script = SCRIPTS_DIR / step["script"]

    print(f"\n{'='*60}")
    print(f"Step: {step_name} — {step['description']}")
    print(f"{'='*60}")

    cmd = [sys.executable, str(script)]

    if step_name == "copy":
        cmd.extend(["--source", str(source), "--dest", str(dest)])
        if dry_run:
            cmd.append("--dry-run")
        if incremental:
            cmd.append("--incremental")
        if section:
            cmd.extend(["--section", section])

    elif step_name in ("transform", "frontmatter", "links"):
        # These operate on the destination directory
        cmd.append(str(dest))
        if dry_run:
            cmd.append("--dry-run")

    elif step_name == "check":
        cmd.append(str(dest))
        cmd.append("--verbose")

    result = subprocess.run(cmd, cwd=SCRIPTS_DIR)

    if result.returncode != 0:
        if step_name == "check":
            print(f"\n⚠ Link check found broken links (expected during migration)")
            return True  # Don't fail the pipeline on broken links
        print(f"\n✗ Step '{step_name}' failed with exit code {result.returncode}")
        return False

    print(f"\n✓ Step '{step_name}' completed successfully")
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Orchestrate the full Nextra-to-Mintlify migration."
    )
    parser.add_argument(
        "--source",
        required=True,
        help="Source directory (docs/content/product/)",
    )
    parser.add_argument(
        "--dest",
        required=True,
        help="Destination directory (docs-mintlify/)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview all changes without writing",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Only process new/changed files",
    )
    parser.add_argument(
        "--section",
        help="Migrate only a specific section",
    )
    parser.add_argument(
        "--steps",
        help=f"Comma-separated list of steps to run. Available: {','.join(DEFAULT_STEPS)}",
    )

    args = parser.parse_args()

    source = Path(args.source).resolve()
    dest = Path(args.dest).resolve()

    if not source.is_dir():
        print(f"Error: Source not found: {source}", file=sys.stderr)
        sys.exit(1)

    steps = args.steps.split(",") if args.steps else DEFAULT_STEPS

    # Validate steps
    for step in steps:
        if step not in STEPS:
            print(f"Error: Unknown step '{step}'. Available: {', '.join(STEPS.keys())}", file=sys.stderr)
            sys.exit(1)

    print(f"Migration: {source} -> {dest}")
    print(f"Steps: {', '.join(steps)}")
    if args.dry_run:
        print("Mode: DRY RUN")
    if args.incremental:
        print("Mode: INCREMENTAL")
    if args.section:
        print(f"Section: {args.section}")

    failed = []
    for step in steps:
        success = run_step(step, source, dest, args.dry_run, args.incremental, args.section)
        if not success:
            failed.append(step)
            print(f"\nStopping due to failure in step '{step}'")
            break

    print(f"\n{'='*60}")
    if failed:
        print(f"Migration completed with failures: {', '.join(failed)}")
        sys.exit(1)
    else:
        print(f"Migration completed successfully! ({len(steps)} steps)")


if __name__ == "__main__":
    main()
