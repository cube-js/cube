#!/usr/bin/env python3
"""
Migrate redirects from Nextra's redirects.json to Mintlify's mint.json format.

Also generates new redirects for pages that changed paths during migration
(e.g., /product/auth/... -> /access-security/...).

Usage:
    # Generate Mintlify redirects from existing redirects.json:
    python migrate_redirects.py /path/to/docs/redirects.json

    # Also generate path-migration redirects:
    python migrate_redirects.py /path/to/docs/redirects.json --with-migration-redirects

    # Output as JSON (for pasting into mint.json):
    python migrate_redirects.py /path/to/docs/redirects.json --format json

    # Output as a standalone redirects array file:
    python migrate_redirects.py /path/to/docs/redirects.json -o redirects.json
"""

import argparse
import json
import sys
from pathlib import Path

# Import path rewriting logic from sibling script
# If running standalone, define the rules inline
try:
    from rewrite_links import rewrite_path, PATH_REWRITES
except ImportError:
    PATH_REWRITES = [
        ("/product/apis-integrations/core-data-apis", "/api-reference/core-data-apis"),
        ("/product/apis-integrations/embed-apis", "/api-reference/embed-apis"),
        ("/product/apis-integrations/control-plane-api", "/api-reference/control-plane-api"),
        ("/product/apis-integrations/orchestration-api", "/api-reference/orchestration-api"),
        ("/product/apis-integrations/javascript-sdk", "/api-reference/javascript-sdk"),
        ("/product/apis-integrations/mcp-server", "/api-reference/mcp-server"),
        ("/product/apis-integrations/recipes", "/api-reference/recipes"),
        ("/product/apis-integrations/microsoft-excel", "/docs/integrations/microsoft-excel"),
        ("/product/apis-integrations/google-sheets", "/docs/integrations/google-sheets"),
        ("/product/apis-integrations/tableau", "/docs/integrations/tableau"),
        ("/product/apis-integrations/power-bi", "/docs/integrations/power-bi"),
        ("/product/apis-integrations/semantic-layer-sync", "/docs/integrations/semantic-layer-sync"),
        ("/product/apis-integrations/snowflake-semantic-views", "/docs/integrations/snowflake-semantic-views"),
        ("/product/apis-integrations", "/api-reference"),
        ("/product/auth/methods", "/access-security/authentication"),
        ("/product/auth", "/access-security/access-control"),
        ("/product/administration/sso", "/access-security/sso"),
        ("/product/administration/users-and-permissions", "/access-security/users-and-permissions"),
        ("/product/administration", "/admin"),
        ("/product/exploration", "/analytics"),
        ("/product/presentation", "/analytics"),
        ("/product/embedding", "/embedding"),
        ("/product/getting-started", "/docs/getting-started"),
        ("/product/configuration", "/docs/configuration"),
        ("/product/data-modeling", "/docs/data-modeling"),
        ("/product/caching", "/docs/caching"),
        ("/product/introduction", "/docs/introduction"),
    ]

    def rewrite_path(old_path: str) -> str:
        for old_prefix, new_prefix in PATH_REWRITES:
            if old_path == old_prefix or old_path.startswith(old_prefix + "/") or old_path.startswith(old_prefix + "#"):
                remainder = old_path[len(old_prefix):]
                return new_prefix + remainder
        if old_path.startswith("/product/"):
            return "/docs/" + old_path[len("/product/"):]
        return old_path


def load_nextra_redirects(filepath: Path) -> list[dict]:
    """Load redirects from Nextra's redirects.json."""
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Nextra format: [{"source": "/old", "destination": "/new", "permanent": true}, ...]
    if isinstance(data, list):
        return data
    # Some formats nest under a key
    if isinstance(data, dict) and "redirects" in data:
        return data["redirects"]
    return data


def convert_redirect(redirect: dict) -> dict:
    """Convert a single Nextra redirect to Mintlify format."""
    source = redirect.get("source", "")
    destination = redirect.get("destination", "")

    # Rewrite both source and destination to new paths
    new_destination = rewrite_path(destination)

    # Mintlify format: {"source": "/old", "destination": "/new"}
    return {
        "source": source,
        "destination": new_destination,
    }


def generate_migration_redirects() -> list[dict]:
    """
    Generate redirects for all pages that moved during migration.

    Creates /product/X -> /new-tab/X redirects so old URLs still work.
    """
    redirects = []

    # Generate wildcard-style redirects for each path prefix change
    for old_prefix, new_prefix in PATH_REWRITES:
        # Exact match redirect
        redirects.append({
            "source": old_prefix,
            "destination": new_prefix,
        })
        # Wildcard redirect for all subpaths
        # Mintlify does not support true wildcards, but we can add
        # the prefix redirect and rely on specific page redirects
        # for known subpaths

    return redirects


def deduplicate_redirects(redirects: list[dict]) -> list[dict]:
    """Remove duplicate redirects, keeping the last one for each source."""
    seen = {}
    for r in redirects:
        source = r["source"]
        seen[source] = r

    return list(seen.values())


def validate_redirects(redirects: list[dict]) -> list[str]:
    """Check for redirect loops and chains."""
    warnings = []
    dest_set = {r["destination"] for r in redirects}
    source_set = {r["source"] for r in redirects}

    for r in redirects:
        # Self-redirect
        if r["source"] == r["destination"]:
            warnings.append(f"Self-redirect: {r['source']}")

        # Chain: destination is also a source
        if r["destination"] in source_set:
            warnings.append(f"Redirect chain: {r['source']} -> {r['destination']} -> ...")

    return warnings


def main():
    parser = argparse.ArgumentParser(
        description="Migrate redirects from Nextra to Mintlify format."
    )
    parser.add_argument(
        "redirects_file",
        help="Path to Nextra's redirects.json",
    )
    parser.add_argument(
        "--with-migration-redirects",
        action="store_true",
        help="Also generate redirects for path changes from migration",
    )
    parser.add_argument(
        "--format",
        choices=["json", "jsonl"],
        default="json",
        help="Output format (default: json)",
    )
    parser.add_argument(
        "-o", "--output",
        help="Output file path (default: stdout)",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate redirects for loops and chains",
    )

    args = parser.parse_args()

    # Load existing redirects
    redirects_path = Path(args.redirects_file)
    if not redirects_path.exists():
        print(f"Error: {redirects_path} not found", file=sys.stderr)
        sys.exit(1)

    nextra_redirects = load_nextra_redirects(redirects_path)
    print(f"Loaded {len(nextra_redirects)} existing redirects", file=sys.stderr)

    # Convert to Mintlify format
    mintlify_redirects = [convert_redirect(r) for r in nextra_redirects]

    # Add migration redirects if requested
    if args.with_migration_redirects:
        migration_redirects = generate_migration_redirects()
        print(f"Generated {len(migration_redirects)} migration redirects", file=sys.stderr)
        mintlify_redirects = migration_redirects + mintlify_redirects

    # Deduplicate
    original_count = len(mintlify_redirects)
    mintlify_redirects = deduplicate_redirects(mintlify_redirects)
    if original_count != len(mintlify_redirects):
        print(
            f"Deduplicated: {original_count} -> {len(mintlify_redirects)}",
            file=sys.stderr,
        )

    # Validate
    if args.validate:
        warnings = validate_redirects(mintlify_redirects)
        if warnings:
            print(f"\n{len(warnings)} warnings:", file=sys.stderr)
            for w in warnings:
                print(f"  - {w}", file=sys.stderr)
        else:
            print("No redirect issues found.", file=sys.stderr)

    # Output
    output = json.dumps(mintlify_redirects, indent=2)

    if args.output:
        Path(args.output).write_text(output, encoding="utf-8")
        print(f"Wrote {len(mintlify_redirects)} redirects to {args.output}", file=sys.stderr)
    else:
        print(output)


if __name__ == "__main__":
    main()
