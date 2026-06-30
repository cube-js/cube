#!/usr/bin/env python3
"""
Transform Nextra custom MDX components to Mintlify equivalents.

Usage:
    # Transform a single file (in-place):
    python transform_components.py path/to/file.mdx

    # Transform a directory recursively:
    python transform_components.py path/to/docs/

    # Dry run (show changes without writing):
    python transform_components.py --dry-run path/to/docs/

    # Pipe mode (stdin/stdout):
    cat file.mdx | python transform_components.py --stdin
"""

import argparse
import os
import re
import sys
from pathlib import Path


def transform_alert_boxes(content: str) -> str:
    """InfoBox -> Info, WarningBox -> Warning, SuccessBox -> Tip, ReferenceBox -> Note."""
    replacements = {
        "InfoBox": "Info",
        "WarningBox": "Warning",
        "SuccessBox": "Tip",
        "ReferenceBox": "Note",
    }
    for nextra, mintlify in replacements.items():
        content = content.replace(f"<{nextra}>", f"<{mintlify}>")
        content = content.replace(f"</{nextra}>", f"</{mintlify}>")
        # Also handle tags with attributes: <InfoBox heading="..."> -> <Info>
        content = re.sub(
            rf"<{nextra}\s+[^>]*>",
            f"<{mintlify}>",
            content,
        )
    return content


def transform_codetabs(content: str) -> str:
    """
    Transform <CodeTabs> to <CodeGroup>.

    Mintlify's CodeGroup requires a title on each code block.
    We add title attributes based on the language or filename.
    """
    content = content.replace("<CodeTabs>", "<CodeGroup>")
    content = content.replace("</CodeTabs>", "</CodeGroup>")

    # Language display names
    lang_names = {
        "yaml": "YAML",
        "yml": "YAML",
        "javascript": "JavaScript",
        "js": "JavaScript",
        "typescript": "TypeScript",
        "ts": "TypeScript",
        "python": "Python",
        "py": "Python",
        "bash": "Bash",
        "sh": "Shell",
        "shell": "Shell",
        "sql": "SQL",
        "json": "JSON",
        "jsx": "JSX",
        "tsx": "TSX",
        "graphql": "GraphQL",
        "ruby": "Ruby",
        "go": "Go",
        "rust": "Rust",
        "java": "Java",
        "toml": "TOML",
        "ini": "INI",
        "text": "Text",
        "plaintext": "Text",
        "csv": "CSV",
        "xml": "XML",
        "html": "HTML",
        "css": "CSS",
        "scss": "SCSS",
        "dockerfile": "Dockerfile",
        "docker": "Docker",
        "nginx": "Nginx",
        "env": ".env",
        "dotenv": ".env",
        "r": "R",
        "swift": "Swift",
        "kotlin": "Kotlin",
        "php": "PHP",
        "lua": "Lua",
        "dart": "Dart",
        "c": "C",
        "cpp": "C++",
        "csharp": "C#",
    }

    def add_code_block_title(match):
        """Add title to code blocks inside CodeGroup sections."""
        backticks = match.group(1)
        lang = match.group(2) or ""
        rest = match.group(3) or ""

        # If there's already a filename attribute, use it as the title
        filename_match = re.search(r'filename="([^"]+)"', rest)
        if filename_match:
            # Mintlify uses title= instead of filename=
            filename = filename_match.group(1)
            rest_without_filename = re.sub(r'\s*filename="[^"]+"', "", rest)
            return f'{backticks}{lang} title="{filename}"{rest_without_filename}'

        # If already has a title, leave it alone
        if "title=" in rest:
            return match.group(0)

        # Add title based on language
        title = lang_names.get(lang.strip(), lang.strip().capitalize() if lang.strip() else "")
        if title:
            return f'{backticks}{lang} title="{title}"{rest}'

        return match.group(0)

    # Priority order for code blocks: YAML first, then Python, then rest
    LANG_PRIORITY = {"yaml": 0, "yml": 0, "python": 1, "py": 1}

    def reorder_code_blocks(group_content):
        """Reorder code blocks so YAML comes first, then Python, then others."""
        # Split into individual code blocks
        block_pattern = re.compile(r"(```+\w*.*?```+)", re.DOTALL)
        blocks = block_pattern.findall(group_content)
        if len(blocks) <= 1:
            return group_content

        def block_sort_key(block):
            lang_match = re.match(r"```+(\w+)", block)
            lang = lang_match.group(1).lower() if lang_match else ""
            return LANG_PRIORITY.get(lang, 99)

        sorted_blocks = sorted(blocks, key=block_sort_key)
        if sorted_blocks == blocks:
            return group_content

        # Rebuild with sorted blocks
        result = "\n\n".join(sorted_blocks)
        return "\n\n" + result + "\n\n"

    # Only add titles to code blocks that are inside <CodeGroup> sections
    def process_codegroup(match):
        group_content = match.group(1)
        # Reorder so YAML is first
        group_content = reorder_code_blocks(group_content)
        # Add title to code fences inside the group
        group_content = re.sub(
            r"(```+)(\w+)?(.*?)$",
            add_code_block_title,
            group_content,
            flags=re.MULTILINE,
        )
        return f"<CodeGroup>{group_content}</CodeGroup>"

    content = re.sub(
        r"<CodeGroup>(.*?)</CodeGroup>",
        process_codegroup,
        content,
        flags=re.DOTALL,
    )

    return content


def transform_screenshot(content: str) -> str:
    """
    Transform <Screenshot> to <Frame><img /></Frame>.

    Handles both self-closing and multiline forms:
      <Screenshot src="..." />
      <Screenshot
        alt="..."
        src="..."
      />
    """

    def screenshot_replacer(match):
        tag_content = match.group(1)
        src_match = re.search(r'src="([^"]+)"', tag_content)
        alt_match = re.search(r'alt="([^"]+)"', tag_content)

        src = src_match.group(1) if src_match else ""
        alt = alt_match.group(1) if alt_match else ""

        if alt:
            return f'<Frame>\n  <img src="{src}" alt="{alt}" />\n</Frame>'
        else:
            return f'<Frame>\n  <img src="{src}" />\n</Frame>'

    # Match self-closing and multiline Screenshot tags
    content = re.sub(
        r"<Screenshot\s+(.*?)\s*/?>",
        screenshot_replacer,
        content,
        flags=re.DOTALL,
    )

    return content


def transform_diagram(content: str) -> str:
    """Transform <Diagram> to <Frame><img /></Frame>."""

    def diagram_replacer(match):
        tag_content = match.group(1)
        src_match = re.search(r'src="([^"]+)"', tag_content)
        alt_match = re.search(r'alt="([^"]+)"', tag_content)

        src = src_match.group(1) if src_match else ""
        alt = alt_match.group(1) if alt_match else ""

        if alt:
            return f'<Frame caption="{alt}">\n  <img src="{src}" alt="{alt}" />\n</Frame>'
        else:
            return f'<Frame>\n  <img src="{src}" />\n</Frame>'

    content = re.sub(
        r"<Diagram\s+(.*?)\s*/?>",
        diagram_replacer,
        content,
        flags=re.DOTALL,
    )

    return content


def transform_youtube(content: str) -> str:
    """Transform <YouTubeVideo> to an iframe."""

    def youtube_replacer(match):
        tag_content = match.group(1)
        url_match = re.search(r'url="([^"]+)"', tag_content)
        url = url_match.group(1) if url_match else ""

        return (
            f'<iframe\n'
            f'  width="100%"\n'
            f'  height="400"\n'
            f'  src="{url}"\n'
            f'  title="YouTube video"\n'
            f'  frameBorder="0"\n'
            f'  allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"\n'
            f'  allowFullScreen\n'
            f'/>'
        )

    content = re.sub(
        r"<YouTubeVideo\s+(.*?)\s*/?>",
        youtube_replacer,
        content,
        flags=re.DOTALL,
    )

    return content


def transform_loom(content: str) -> str:
    """Transform <LoomVideo> to an iframe."""

    def loom_replacer(match):
        tag_content = match.group(1)
        url_match = re.search(r'url="([^"]+)"', tag_content)
        url = url_match.group(1) if url_match else ""

        return (
            f'<iframe\n'
            f'  width="100%"\n'
            f'  height="400"\n'
            f'  src="{url}"\n'
            f'  title="Loom video"\n'
            f'  frameBorder="0"\n'
            f'  allowFullScreen\n'
            f'/>'
        )

    content = re.sub(
        r"<LoomVideo\s+(.*?)\s*/?>",
        loom_replacer,
        content,
        flags=re.DOTALL,
    )

    return content


def transform_grid(content: str) -> str:
    """
    Transform <Grid>/<GridItem> to <CardGroup>/<Card>.

    <Grid cols={2} imageSize={[56, 56]}>
      <GridItem url="path" imageUrl="icon.svg" title="Title" />
    </Grid>

    becomes:

    <CardGroup cols={2}>
      <Card title="Title" icon="link" href="path">
      </Card>
    </CardGroup>
    """

    def grid_replacer(match):
        tag_attrs = match.group(1)
        inner = match.group(2)

        # Extract cols
        cols_match = re.search(r"cols=\{(\d+)\}", tag_attrs)
        cols = cols_match.group(1) if cols_match else "2"

        # Transform GridItems inside
        def griditem_replacer(gi_match):
            gi_content = gi_match.group(1)
            url_match = re.search(r'url="([^"]+)"', gi_content)
            title_match = re.search(r'title="([^"]+)"', gi_content)
            img_match = re.search(r'imageUrl="([^"]+)"', gi_content)

            url = url_match.group(1) if url_match else ""
            title = title_match.group(1) if title_match else ""
            img = img_match.group(1) if img_match else ""

            parts = [f'<Card title="{title}"']
            if img:
                parts.append(f' img="{img}"')
            if url:
                parts.append(f' href="{url}"')
            parts.append(">\n  </Card>")
            return "".join(parts)

        inner = re.sub(
            r"<GridItem\s+(.*?)\s*/?>",
            griditem_replacer,
            inner,
            flags=re.DOTALL,
        )

        return f"<CardGroup cols={{{cols}}}>{inner}</CardGroup>"

    content = re.sub(
        r"<Grid\s*(.*?)>(.*?)</Grid>",
        grid_replacer,
        content,
        flags=re.DOTALL,
    )

    return content


def transform_envvar(content: str) -> str:
    """
    Transform <EnvVar>VAR_NAME</EnvVar> to a code-linked reference.

    Becomes: [`VAR_NAME`](/docs/configuration/reference/environment-variables#var_name)
    """

    def envvar_replacer(match):
        var_name = match.group(1)
        anchor = var_name.lower().replace("_", "_")
        return f"[`{var_name}`](/docs/configuration/reference/environment-variables#{anchor})"

    content = re.sub(
        r"<EnvVar>([^<]+)</EnvVar>",
        envvar_replacer,
        content,
    )

    return content


def transform_btn(content: str) -> str:
    """Transform <Btn>Settings → Configuration</Btn> to **Settings → Configuration**."""
    content = re.sub(
        r"<Btn>([^<]+)</Btn>",
        r"**\1**",
        content,
    )
    return content


def transform_community_driver(content: str) -> str:
    """
    Transform <CommunitySupportedDriver dataSource="X" /> to a Warning callout.
    """

    def driver_replacer(match):
        datasource = match.group(1)
        return (
            f"<Warning>\n\n"
            f"The driver for {datasource} is community-supported and is not "
            f"maintained by Cube or the database vendor.\n\n"
            f"</Warning>"
        )

    content = re.sub(
        r'<CommunitySupportedDriver\s+dataSource="([^"]+)"\s*/?>',
        driver_replacer,
        content,
    )

    return content


def transform_product_video(content: str) -> str:
    """Transform <ProductVideo url="..."> to an iframe."""

    def video_replacer(match):
        tag_content = match.group(1)
        url_match = re.search(r'url="([^"]+)"', tag_content)
        url = url_match.group(1) if url_match else ""

        return (
            f'<iframe\n'
            f'  width="100%"\n'
            f'  height="400"\n'
            f'  src="{url}"\n'
            f'  title="Product video"\n'
            f'  frameBorder="0"\n'
            f'  allowFullScreen\n'
            f'/>'
        )

    content = re.sub(
        r"<ProductVideo\s+(.*?)\s*/?>",
        video_replacer,
        content,
        flags=re.DOTALL,
    )

    return content


def transform_filename_to_title(content: str) -> str:
    """
    Transform filename= attribute on code blocks to title= for Mintlify.

    ```python filename="cube.py"  ->  ```python title="cube.py"

    Only applies to code blocks NOT inside a CodeGroup (those are handled
    by transform_codetabs).
    """
    # Find code blocks that are NOT inside CodeGroup
    # We do a simple replacement since CodeGroup blocks are already processed
    # and their filename= attrs are already converted

    def replacer(match):
        prefix = match.group(1)
        filename = match.group(2)
        rest = match.group(3)
        return f'{prefix}title="{filename}"{rest}'

    content = re.sub(
        r'(```\w+)\s+filename="([^"]+)"(.*?)$',
        replacer,
        content,
        flags=re.MULTILINE,
    )

    return content


def transform_all(content: str) -> str:
    """Apply all component transformations in order."""
    content = transform_alert_boxes(content)
    content = transform_codetabs(content)
    content = transform_screenshot(content)
    content = transform_diagram(content)
    content = transform_youtube(content)
    content = transform_loom(content)
    content = transform_product_video(content)
    content = transform_grid(content)
    content = transform_envvar(content)
    content = transform_btn(content)
    content = transform_community_driver(content)
    content = transform_filename_to_title(content)
    return content


def process_file(filepath: Path, dry_run: bool = False) -> bool:
    """Process a single MDX file. Returns True if changes were made."""
    content = filepath.read_text(encoding="utf-8")
    transformed = transform_all(content)

    if content == transformed:
        return False

    if dry_run:
        print(f"[WOULD CHANGE] {filepath}")
        # Show a unified-diff-like summary
        orig_lines = content.splitlines()
        new_lines = transformed.splitlines()
        changes = 0
        for i, (old, new) in enumerate(zip(orig_lines, new_lines)):
            if old != new:
                changes += 1
                if changes <= 10:
                    print(f"  L{i+1}:")
                    print(f"    - {old.strip()}")
                    print(f"    + {new.strip()}")
        if changes > 10:
            print(f"  ... and {changes - 10} more changes")
        print()
    else:
        filepath.write_text(transformed, encoding="utf-8")
        print(f"[UPDATED] {filepath}")

    return True


def main():
    parser = argparse.ArgumentParser(
        description="Transform Nextra MDX components to Mintlify equivalents."
    )
    parser.add_argument(
        "path",
        nargs="?",
        help="File or directory to transform",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would change without writing files",
    )
    parser.add_argument(
        "--stdin",
        action="store_true",
        help="Read from stdin, write to stdout",
    )

    args = parser.parse_args()

    if args.stdin:
        content = sys.stdin.read()
        sys.stdout.write(transform_all(content))
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
