#!/usr/bin/env python3
"""Fail when a docs page references media that isn't hosted on static.cube.dev.

Images and video are uploaded with scripts/upload-asset.sh and referenced as
https://static.cube.dev/<key>. This check catches media committed to the repo
(`/images/...`) or linked from any other host (Uploadcare, S3, Drive, ...).

Checked:
  - src / poster attributes on <img>, <video>, <source> and <audio>
  - Markdown images: ![alt](url)
  - <iframe src>, which must point to an allowlisted video player

Usage: python3 scripts/check_media_hosts.py [<docs root>]
"""

import os
import re
import sys

MEDIA_PREFIX = "https://static.cube.dev/"

# Players that serve their own content; they embed a page, not a media file.
IFRAME_HOSTS = (
    "https://www.youtube-nocookie.com/",
    "https://www.youtube.com/",
    "https://www.loom.com/",
    "https://codesandbox.io/",
)

MEDIA_TAG = re.compile(r"<(?:img|video|source|audio)\b[^>]*>", re.IGNORECASE | re.DOTALL)
IFRAME_TAG = re.compile(r"<iframe\b[^>]*>", re.IGNORECASE | re.DOTALL)
ATTR = re.compile(r"\b(src|poster)\s*=\s*(?:\"([^\"]*)\"|'([^']*)'|\{\s*[\"'`]([^\"'`]*)[\"'`]\s*\})")
MD_IMAGE = re.compile(r"!\[[^\]]*\]\(\s*<?([^)\s>]+)")
FENCE = re.compile(r"^\s*(```|~~~)")
MDX_COMMENT = re.compile(r"\{/\*.*?\*/\}", re.DOTALL)
INLINE_CODE = re.compile(r"`[^`\n]*`")


def blank(match):
    return re.sub(r"[^\n]", " ", match.group(0))


def strip_non_content(text):
    """Blank out code and MDX comments so examples and placeholders don't count, keeping line numbers."""
    out, in_fence = [], False
    for line in text.split("\n"):
        if FENCE.match(line):
            in_fence = not in_fence
            out.append("")
        else:
            out.append("" if in_fence else line)
    text = "\n".join(out)
    text = MDX_COMMENT.sub(blank, text)
    return INLINE_CODE.sub(blank, text)


def line_of(text, pos):
    return text.count("\n", 0, pos) + 1


def check_file(path):
    text = strip_non_content(open(path, encoding="utf-8").read())
    problems = []

    for tag in MEDIA_TAG.finditer(text):
        for attr in ATTR.finditer(tag.group(0)):
            url = next(g for g in attr.groups()[1:] if g is not None)
            if not url.startswith(MEDIA_PREFIX):
                problems.append((line_of(text, tag.start()), url))

    for img in MD_IMAGE.finditer(text):
        url = img.group(1)
        if not url.startswith(MEDIA_PREFIX):
            problems.append((line_of(text, img.start()), url))

    for tag in IFRAME_TAG.finditer(text):
        for attr in ATTR.finditer(tag.group(0)):
            if attr.group(1) != "src":
                continue
            url = next(g for g in attr.groups()[1:] if g is not None)
            if not url.startswith(IFRAME_HOSTS):
                problems.append((line_of(text, tag.start()), url))

    return problems


def main():
    root = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(__file__), "..")
    failures = 0
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in ("node_modules", ".git")]
        for name in sorted(filenames):
            if not name.endswith((".mdx", ".md")) or name in ("CLAUDE.md", "README.md"):
                continue
            path = os.path.join(dirpath, name)
            for line, url in check_file(path):
                rel = os.path.relpath(path, root)
                print(f"::error file=docs-mintlify/{rel},line={line}::Media must be hosted on static.cube.dev: {url}")
                print(f"{rel}:{line}: {url}", file=sys.stderr)
                failures += 1

    if failures:
        print(
            f"\n{failures} media reference(s) not on static.cube.dev. Upload each file with "
            "`./scripts/upload-asset.sh <file> <key>` and reference https://static.cube.dev/<key>. "
            "See docs-mintlify/CLAUDE.md, \"Images and screenshots\".",
            file=sys.stderr,
        )
        return 1
    print("All media references point at static.cube.dev.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
