#!/usr/bin/env python3
"""
Build the cross-domain redirect table for the OLD docs site (Next.js/Nextra,
served at cube.dev/docs) so that every legacy URL 301s to the NEW Mintlify docs
at docs.cube.dev.

Why this is content-driven (and not a simple prefix rewrite)
------------------------------------------------------------
An earlier version of this script reused ``rewrite_links.PATH_REWRITES`` to map
old ``/product/...`` paths to new ones. That table described an INTERMEDIATE
migration layout (``/access-security``, ``/api-reference``, ``/analytics`` tabs
and ``/docs/data-modeling/reference/...`` pages) that no longer exists — the
content was later consolidated into the live tabs (``admin``, ``reference``,
``docs``, ``recipes``, ``embedding``, ``configuration``, ``cube-core``). As a
result ~76% of the generated destinations 404'd (e.g.
``/docs/data-modeling/reference/pre-aggregations`` instead of the real
``/reference/data-modeling/pre-aggregations``).

To stay correct as the layout evolves, destinations are derived by MATCHING the
old page's body text against the new Mintlify pages (the new files were copied
from the old ones during migration, so the prose is preserved even though
front matter and links were rewritten). Pages that cannot be matched reliably
(short/list pages, or pages consolidated/removed during migration) are pinned
explicitly in ``OVERRIDES``.

Every emitted destination is validated against the on-disk Mintlify tree; the
script exits non-zero if any destination would 404, so a broken table can never
be committed silently.

Layers produced, in first-match-wins order:

  1. Specific redirects — every entry from the old ``redirects.json`` (legacy
     aliases) plus one direct redirect per canonical old ``/product/...`` page,
     each pointing at its verified new home.

  2. A final ``/product/:path*`` catch-all so any legacy ``/product`` URL that
     isn't enumerated still lands on the new docs instead of 404ing.

``basePath: false`` is intentionally omitted in next.config.mjs. Next.js
prefixes a redirect's ``source`` with the configured ``basePath`` (``/docs``)
only when ``basePath`` is not false, while absolute (http/https) destinations
are treated as external and never get the basePath prepended. Omitting it
therefore matches legacy URLs under ``/docs`` and keeps the cross-domain
destination intact.

Usage:
    python build_old_site_redirects.py \
        --redirects ../docs/redirects.json \
        --old-content ../docs/content/product \
        --new-root .. \
        -o ../docs/redirects-new-docs.json
"""

import argparse
import json
import re
import sys
from pathlib import Path

NEW_DOCS_ORIGIN = "https://docs.cube.dev"

# Tabs/dirs that hold real Mintlify pages. Anything else under docs-mintlify/
# (scripts, images, node_modules, ...) is ignored when indexing/validating.
NEW_CONTENT_DIRS = {
    "admin", "configuration", "cube-core", "docs",
    "embedding", "recipes", "reference",
}

# Content-match tuning. Shingle = sliding window of N normalized words; the
# match score is Jaccard overlap of the old and new shingle sets.
SHINGLE_N = 4

# Pages whose new home cannot be recovered reliably by content matching:
#   - short/list pages with little distinctive prose, or
#   - pages consolidated into another page or removed during migration.
# Keys are old (legacy) URLs; values are root-relative new Mintlify paths that
# are verified to exist. These take precedence over the content match.
OVERRIDES = {
    # AI admin pages renamed/consolidated (mirrors docs.json internal redirects).
    "/product/administration/ai": "/admin/ai",
    "/product/administration/ai/agent-rules": "/admin/ai/rules",
    "/product/administration/ai/spaces-agents-models": "/admin/ai/multi-agent",
    "/product/administration/ai/yaml-config": "/admin/ai",

    # Monitoring landing (no standalone monitoring index in the new tree).
    "/product/administration/monitoring": "/admin/monitoring/query-history",

    # Dedicated-infrastructure (old "vpc"/"byoc") networking pages.
    "/product/administration/deployment/byoc/aws/privatelink": "/admin/deployment/dedicated/aws/private-link",
    "/product/administration/deployment/vpc": "/admin/deployment/dedicated",
    "/product/administration/deployment/vpc/aws": "/admin/deployment/dedicated/aws",
    "/product/administration/deployment/vpc/azure": "/admin/deployment/dedicated/azure",
    "/product/administration/deployment/vpc/gcp": "/admin/deployment/dedicated/gcp",

    # Okta SSO landing -> SAML page (no standalone Okta index in the new tree).
    "/product/administration/sso/okta": "/admin/sso/okta/saml",

    # Workspace pages consolidated/removed; fall back to the closest live home.
    "/product/administration/workspace": "/admin",
    "/product/administration/workspace/cli": "/admin",
    "/product/administration/workspace/cli/reference": "/admin",
    "/product/administration/workspace/preferences": "/admin",
    "/product/administration/workspace/maintenance-window": "/admin",
    "/product/administration/workspace/semantic-catalog": "/admin",
    "/product/administration/workspace/integrations": "/admin/connect-to-data/visualization-tools",
    "/product/administration/workspace/saved-reports": "/docs/explore-analyze/workbooks",

    # Auth methods moved under the Embedding tab's authentication section;
    # provider-specific pages without a dedicated new page go to the SSO/auth
    # landing rather than 404.
    "/product/auth/methods": "/embedding/authentication/jwt",
    "/product/auth/methods/identity-provider": "/admin/sso",
    "/product/auth/methods/name-password": "/embedding/authentication/jwt",

    # Data-modeling concepts landing/short pages.
    "/product/data-modeling/concepts": "/docs/data-modeling/overview",
    "/product/data-modeling/concepts/calculated-members": "/docs/data-modeling/measures",
    "/product/data-modeling/dynamic": "/docs/data-modeling/dynamic",
    "/product/data-modeling/advanced/code-reusability-export-and-import": "/docs/data-modeling/dynamic/code-reusability-export-and-import",
    "/product/data-modeling/advanced/polymorphic-cubes": "/recipes/data-modeling/polymorphic-cubes",

    # Data-model reference landing and the removed types-and-formats page.
    "/product/data-modeling/reference": "/reference",
    "/product/data-modeling/reference/types-and-formats": "/reference/data-modeling/dimensions",

    # FAQ section removed; send to the docs entry point.
    "/product/faqs/general": "/docs/introduction",
    "/product/faqs/tips-and-tricks": "/docs/introduction",
    "/product/faqs/troubleshooting": "/docs/introduction",

    # Legacy guide alias whose canonical page no longer exists under /product.
    "/guides/recipes/code-reusability/using-dynamic-measures": "/recipes/data-modeling/using-dynamic-measures",

    # Migrate-from-Core landing (no index; point at the most common path).
    "/product/getting-started/migrate-from-core": "/docs/getting-started/migrate-from-core/import-github-repository",

    # Metabase semantic-layer sync was removed; send to the sync overview.
    "/product/apis-integrations/semantic-layer-sync/metabase": "/docs/integrations/semantic-layer-sync",
}

# Where to send any /product/* URL that matches nothing else.
PRODUCT_CATCH_ALL = "/docs/introduction"


# --------------------------------------------------------------------------- #
# Content matching
# --------------------------------------------------------------------------- #

def normalize(text: str) -> str:
    """Reduce MDX to a bag of prose words, ignoring front matter/markup/links."""
    text = re.sub(r"^---\n.*?\n---\n", "", text, flags=re.S)      # front matter
    text = re.sub(r"^(import|export)\s.*$", "", text, flags=re.M)  # JS imports
    text = re.sub(r"<[^>]+>", " ", text)                           # JSX tags
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)           # [t](url)->t
    text = re.sub(r"^\[[^\]]+\]:\s*\S+\s*$", "", text, flags=re.M)  # ref links
    text = text.replace("`", "")
    text = re.sub(r"[#>*_\-]", " ", text)
    return re.sub(r"\s+", " ", text).lower().strip()


def shingles(text: str) -> set:
    words = normalize(text).split()
    if len(words) < SHINGLE_N:
        return set(words)
    return {" ".join(words[i:i + SHINGLE_N]) for i in range(len(words) - SHINGLE_N + 1)}


def file_to_url(path: Path, root: Path) -> str:
    """Map a Mintlify .mdx file to its served URL path (root-relative)."""
    rel = str(path.relative_to(root)).removesuffix(".mdx")
    rel = re.sub(r"/index$", "", rel)
    return "/" + rel


def index_new_pages(new_root: Path) -> list:
    """Return [(url, shingle_set, valid_path_set_entry)] for every new page."""
    pages = []
    for d in sorted(NEW_CONTENT_DIRS):
        base = new_root / d
        if not base.is_dir():
            continue
        for f in sorted(base.rglob("*.mdx")):
            text = f.read_text(encoding="utf-8", errors="ignore")
            pages.append((file_to_url(f, new_root), shingles(text)))
    return pages


def build_page_map(old_content: Path, new_root: Path, new_pages: list) -> dict:
    """Map each canonical old /product/... URL to its verified new URL."""
    page_map = {}
    for f in sorted(old_content.rglob("*.mdx")):
        rel = str(f.relative_to(old_content)).removesuffix(".mdx")
        rel = re.sub(r"/index$", "", rel)
        old_url = "/product/" + rel if rel else "/product"

        if old_url in OVERRIDES:
            page_map[old_url] = OVERRIDES[old_url]
            continue

        osh = shingles(f.read_text(encoding="utf-8", errors="ignore"))
        best_url, best_score = None, 0.0
        for nurl, nsh in new_pages:
            if not osh or not nsh:
                continue
            inter = len(osh & nsh)
            if not inter:
                continue
            score = inter / len(osh | nsh)
            if score > best_score:
                best_score, best_url = score, nurl
        if best_url is not None:
            page_map[old_url] = best_url
    return page_map


# --------------------------------------------------------------------------- #
# Redirect table assembly
# --------------------------------------------------------------------------- #

def to_absolute(path: str) -> str:
    if path.startswith(("http://", "https://")):
        return path
    return NEW_DOCS_ORIGIN + path


def split_fragment(url: str):
    if "#" in url:
        base, frag = url.split("#", 1)
        return base, "#" + frag
    return url, ""


def resolve_destination(old_dest: str, page_map: dict) -> str:
    """Resolve a legacy redirect destination to a new (absolute) URL."""
    if old_dest.startswith(("http://", "https://")):
        return old_dest  # already external (e.g. blog links)

    base, frag = split_fragment(old_dest)
    if base in OVERRIDES:
        new_base = OVERRIDES[base]
    elif base in page_map:
        new_base = page_map[base]
    elif base.startswith("/product/"):
        new_base = PRODUCT_CATCH_ALL
    else:
        new_base = PRODUCT_CATCH_ALL
    return to_absolute(new_base + frag)


def build_redirects(old_redirects: list, page_map: dict) -> list:
    out, seen = [], set()

    def add(source: str, dest: str):
        if source in seen:
            return
        seen.add(source)
        out.append({"source": source, "destination": dest, "permanent": True})

    # 1. Legacy aliases from the old redirects.json.
    for r in old_redirects:
        add(r.get("source", ""), resolve_destination(r.get("destination", ""), page_map))

    # 2. Direct redirects for every canonical old /product page.
    for old_url, new_url in sorted(page_map.items()):
        add(old_url, to_absolute(new_url))

    # 3. Catch-all so unmatched /product URLs never 404.
    add("/product/:path*", to_absolute(PRODUCT_CATCH_ALL))

    return out


def valid_url_set(new_root: Path) -> set:
    urls = set()
    for d in sorted(NEW_CONTENT_DIRS):
        base = new_root / d
        if not base.is_dir():
            continue
        for f in base.rglob("*.mdx"):
            urls.add(file_to_url(f, new_root))
    return urls


def validate(redirects: list, valid: set) -> list:
    """Return a list of redirects whose destination would 404."""
    broken = []
    for r in redirects:
        dest = r["destination"]
        if not dest.startswith(NEW_DOCS_ORIGIN):
            continue  # external destination, not ours to validate
        path = dest[len(NEW_DOCS_ORIGIN):]
        base = split_fragment(path)[0]
        if base.endswith("/:path*"):
            base = base[:-len("/:path*")]
        if base in ("", "/"):
            continue
        if base not in valid:
            broken.append((r["source"], path))
    return broken


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--redirects", default="../docs/redirects.json")
    parser.add_argument("--old-content", default="../docs/content/product")
    parser.add_argument("--new-root", default="..")
    parser.add_argument("-o", "--output", default="../docs/redirects-new-docs.json")
    args = parser.parse_args()

    redirects_path = Path(args.redirects)
    old_content = Path(args.old_content)
    new_root = Path(args.new_root)

    for p in (redirects_path, old_content, new_root):
        if not p.exists():
            print(f"Error: {p} not found", file=sys.stderr)
            sys.exit(1)

    old_redirects = json.loads(redirects_path.read_text(encoding="utf-8"))

    print("Indexing new Mintlify pages...", file=sys.stderr)
    new_pages = index_new_pages(new_root)
    print(f"  {len(new_pages)} new pages", file=sys.stderr)

    print("Content-matching old pages to new pages...", file=sys.stderr)
    page_map = build_page_map(old_content, new_root, new_pages)
    print(f"  mapped {len(page_map)} old pages", file=sys.stderr)

    redirects = build_redirects(old_redirects, page_map)

    valid = valid_url_set(new_root)
    broken = validate(redirects, valid)
    if broken:
        print(f"\nERROR: {len(broken)} redirect destinations do not resolve:",
              file=sys.stderr)
        for s, d in broken[:50]:
            print(f"  {s}  ->  {d}", file=sys.stderr)
        sys.exit(2)

    Path(args.output).write_text(
        json.dumps(redirects, indent=2) + "\n", encoding="utf-8")
    print(f"\nWrote {len(redirects)} redirects to {args.output} "
          f"(all destinations validated against the live tree).", file=sys.stderr)


if __name__ == "__main__":
    main()
