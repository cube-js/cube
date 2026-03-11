# Cube Documentation

This file provides guidance to Claude Code when working with the documentation site.

## Writing Style

**Tone**: Professional, direct, and instructive. Address the reader as "you" in second person.

**Good**: "You can connect a Cube deployment to Metabase using the SQL API."
**Avoid**: "One can connect..." or "Users can connect..."

**Headings**:
- H1 (`#`) for page title only (one per page)
- H2 (`##`) for major sections
- H3 (`###`) for subsections
- H4 (`####`) rarely, only for deep nesting

**Code**:
- Always specify language: ` ```python`, ` ```yaml`, ` ```javascript`
- Use `filename=` attribute when helpful: ` ```python filename="cube.py"`
- Inline code with backticks for identifiers: `driver_factory`, `security_context`, `pre_aggregations`

**Links**:
- Define references at file bottom:
  ```
  [ref-config]: /product/configuration
  [ref-env-vars]: /product/configuration/reference/environment-variables
  ```
- Use reference syntax inline: `[configuration options][ref-config]`

**Paragraphs**: Keep moderate length (3-4 sentences). Use bullet lists (with `-`) for multiple items.

## Custom Components

### Alert Boxes

Use for callouts. Content should be on separate lines from the tags.

**InfoBox** — informational notes:
```mdx
<InfoBox>

Scheduled refreshes are available on [Premium and Enterprise plans](https://cube.dev/pricing).

</InfoBox>
```

**WarningBox** — important warnings:
```mdx
<WarningBox>

Cube expects the context to be an object. If you don't provide an object as the
JWT payload, you will receive an error.

</WarningBox>
```

**SuccessBox** — availability or positive notes:
```mdx
<SuccessBox>

Presentation tools are available in both Cube Cloud and Cube Core.

</SuccessBox>
```

**ReferenceBox** — links to related documentation:
```mdx
<ReferenceBox>

See [Cube style guide][ref-style-guide] for more recommendations on syntax and structure.

</ReferenceBox>
```

### Code Tabs (for multi-language examples)

````mdx
<CodeTabs>

```python
from cube import config
```

```javascript
const config = {}
```

</CodeTabs>
````

### UI Navigation

```mdx
<Btn>Settings → Configuration</Btn>
```

### Environment Variables

```mdx
<EnvVar>CUBEJS_DB_SSL</EnvVar>
```

Auto-links to the environment variables reference.

### Images

```mdx
<Screenshot
  alt="Cube Cloud Environment Variables Screen"
  src="https://ucarecdn.com/..."
/>

<Diagram alt="Architecture diagram" src="..." />
```

### Videos

```mdx
<YouTubeVideo url="https://www.youtube.com/embed/..." />
<LoomVideo url="https://www.loom.com/embed/..." />
```

### Grids (for navigation cards)

```mdx
<Grid cols={2}>
  <GridItem
    url="path/to/page"
    imageUrl="https://static.cube.dev/icons/icon.svg"
    title="Page Title"
  />
</Grid>
```

### Community Drivers

```mdx
<CommunitySupportedDriver dataSource="MongoDB" />
```

## Documentation Structure

### File Organization

- Content lives in `/content/product/`
- Each directory needs `_meta.js` for navigation
- Use `index.mdx` with `asIndexPage: true` frontmatter for section overviews

### _meta.js Files

Define navigation order and display names:

```javascript
export default {
  "introduction": "Introduction",
  "getting-started": "Getting started",
  "configuration": "Data Sources & Config"
}
```

Hide pages from navigation:

```javascript
export default {
  "visible-page": "Visible Page",
  "hidden-page": {
    title: "Hidden Page",
    display: "hidden"
  }
}
```

### index.mdx Files

Create section landing pages:

```mdx
---
asIndexPage: true
---

# Section Title

Overview content here...
```

### URL Mapping

File paths map directly to URLs:
- `configuration/data-sources/postgres.mdx` → `/product/configuration/data-sources/postgres`

## Redirects

When moving or renaming pages, add redirects to `redirects.json`:

```json
{
  "source": "/old/path",
  "destination": "/new/path",
  "permanent": true
}
```

Always use `"permanent": true` for documentation moves.
