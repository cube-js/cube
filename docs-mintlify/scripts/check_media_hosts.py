#!/usr/bin/env python3
"""Fail when a docs page references media that isn't hosted on static.cube.dev.

Images and video are uploaded with scripts/upload-asset.sh and referenced as
https://static.cube.dev/<key>. This check catches media committed to the repo
(`/images/...`) or linked from any other host (Uploadcare, S3, Drive, ...).

Checked:
  - src / poster attributes on <img>, <video>, <source> and <audio>
  - img attributes on components, such as <Card img="...">
  - Those attributes must be string literals: src={url} can't be checked, so it fails
  - Markdown images: ![alt](url), and reference-style ![alt][label] through [label]: url
  - <iframe src>, which must point to an allowlisted video player

Usage: python3 scripts/check_media_hosts.py [<docs root>]
"""

import os
import re
import sys

# Docs about the docs, not pages.
DOCS_DIR = os.path.realpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
EXCLUDED = {os.path.join(DOCS_DIR, "CLAUDE.md"), os.path.join(DOCS_DIR, "scripts", "README.md")}

MEDIA_PREFIX = "https://static.cube.dev/"

# Players that serve their own content; they embed a page, not a media file.
# YouTube only through youtube-nocookie.com: youtube.com sets cookies on page load (see CLAUDE.md).
IFRAME_HOSTS = (
    "https://www.youtube-nocookie.com/",
    "https://www.loom.com/",
    "https://codesandbox.io/",
)

# A tag body skips quoted strings, so a `>` inside an attribute value doesn't end the tag.
# A quoted string can't cross a `<`, so an unbalanced quote (alt=It's) falls back to a plain
# character instead of hiding the tag or scanning on through the rest of the file.
TAG_BODY = r"""(?:"[^"<]*"|'[^'<]*'|[^>])*>"""
MEDIA_TAG = re.compile(r"<(?:img|video|source|audio)\b" + TAG_BODY, re.IGNORECASE | re.DOTALL)
IFRAME_TAG = re.compile(r"<iframe\b" + TAG_BODY, re.IGNORECASE | re.DOTALL)
COMPONENT_TAG = re.compile(r"<[A-Z][A-Za-z0-9.]*\b" + TAG_BODY, re.DOTALL)
ATTR = re.compile(r"(?<![-\w])(src|poster|img)\s*=\s*(?:\"([^\"]*)\"|'([^']*)'|\{\s*[\"']([^\"']*)[\"']\s*\})")
# An {expression} value that ATTR can't read as a literal, such as src={url} or src={"/a/" + b}, can't be checked.
EXPR_ATTR = re.compile(r"(?<![-\w])(src|poster|img)\s*=\s*\{")
MD_IMAGE = re.compile(r"!\[[^\]]*\]\(\s*<?([^)\s>]+)")
# Reference-style images: ![alt][label], ![label][] and ![label], resolved through [label]: url.
MD_REF_IMAGE = re.compile(r"!\[([^\]]*)\](?:\[([^\]]*)\])?(?!\()")
MD_REF_DEF = re.compile(r"^ {0,3}\[([^\]]+)\]:\s*<?([^\s>]+)", re.MULTILINE)
FENCE = re.compile(r"^\s*(`{3,}|~{3,})")
MDX_COMMENT = re.compile(r"\{/\*.*?\*/\}", re.DOTALL)
INLINE_CODE = re.compile(r"`[^`\n]*`")


def blank(match):
    return re.sub(r"[^\n]", " ", match.group(0))


def strip_non_content(text):
    """Blank out code and MDX comments so examples and placeholders don't count, keeping line numbers."""
    out, fence = [], None
    for line in text.split("\n"):
        if fence is None and (m := FENCE.match(line)):
            # As in CommonMark, only the same character, at least as long, closes the block.
            fence = re.compile(rf"\s*{re.escape(m.group(1)[0])}{{{len(m.group(1))},}}\s*")
            out.append("")
        elif fence is not None:
            if fence.fullmatch(line):
                fence = None
            out.append("")
        else:
            out.append(line)
    text = "\n".join(out)
    text = MDX_COMMENT.sub(blank, text)
    return INLINE_CODE.sub(blank, text)


def normalize_label(label):
    return " ".join(label.split()).casefold()


def escape_command(value):
    """Escape a value for a GitHub workflow command, so it can't end the command or start another."""
    return value.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def line_of(text, pos):
    return text.count("\n", 0, pos) + 1


def attr_problems(text, tag, names, allowed):
    problems, literal = [], set()
    body = tag.group(0)
    for attr in ATTR.finditer(body):
        literal.add(attr.start())
        if attr.group(1) not in names:
            continue
        url = next(g for g in attr.groups()[1:] if g is not None)
        if not url.startswith(allowed):
            problems.append((line_of(text, tag.start() + attr.start()), url))
    for attr in EXPR_ATTR.finditer(body):
        if attr.group(1) in names and attr.start() not in literal:
            problems.append(
                (line_of(text, tag.start() + attr.start()), f"{attr.group(1)}={{...}} (use a literal static.cube.dev URL)")
            )
    return problems


def check_file(path):
    text = strip_non_content(open(path, encoding="utf-8").read())
    problems = []

    for tag in MEDIA_TAG.finditer(text):
        problems += attr_problems(text, tag, ("src", "poster"), MEDIA_PREFIX)

    for tag in COMPONENT_TAG.finditer(text):
        problems += attr_problems(text, tag, ("img",), MEDIA_PREFIX)

    for img in MD_IMAGE.finditer(text):
        url = img.group(1)
        if not url.startswith(MEDIA_PREFIX):
            problems.append((line_of(text, img.start()), url))

    definitions = {}
    for d in MD_REF_DEF.finditer(text):
        definitions.setdefault(normalize_label(d.group(1)), d.group(2))
    for img in MD_REF_IMAGE.finditer(text):
        url = definitions.get(normalize_label(img.group(2) or img.group(1)))
        if url is not None and not url.startswith(MEDIA_PREFIX):
            problems.append((line_of(text, img.start()), url))

    for tag in IFRAME_TAG.finditer(text):
        problems += attr_problems(text, tag, ("src",), IFRAME_HOSTS)

    problems.sort()
    return problems


def main():
    root = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(__file__), "..")
    if not os.path.isdir(root):
        print(f"Docs root not found: {root}", file=sys.stderr)
        return 1
    failures, scanned = 0, 0
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in ("node_modules", ".git")]
        for name in sorted(filenames):
            path = os.path.join(dirpath, name)
            if not name.endswith((".mdx", ".md")) or os.path.realpath(path) in EXCLUDED:
                continue
            scanned += 1
            for line, url in check_file(path):
                rel = os.path.relpath(path, root)
                print(f"::error file=docs-mintlify/{rel},line={line}::Media must be hosted on static.cube.dev: {escape_command(url)}")
                print(f"{rel}:{line}: {url}", file=sys.stderr)
                failures += 1

    if scanned == 0:
        print(f"No .md/.mdx files found under {root}; nothing was checked.", file=sys.stderr)
        return 1
    if failures:
        print(
            f"\n{failures} media reference(s) not on static.cube.dev. Upload each file with "
            "`./scripts/upload-asset.sh <file> <key>` and reference https://static.cube.dev/<key>. "
            "See docs-mintlify/CLAUDE.md, \"Images and screenshots\".",
            file=sys.stderr,
        )
        return 1
    print(f"All media references in {scanned} files point at static.cube.dev.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
