# Cube Documentation (Mintlify)

This is the **active** Cube documentation site, built with [Mintlify](https://mintlify.com).
All documentation work should happen here.

> The `/docs` directory at the repo root is the **legacy** Nextra docs site and is
> **deprecated** — do not add or edit content there.

## Local development

```bash
cd docs-mintlify
yarn dev    # Start the Mintlify dev server
```

## Naming conventions

Product naming conventions (product names, taxonomy, deployment types, plan
tiers, API names) are maintained in a shared rules file — follow it in all
docs content:

@../.cursor/rules/namings-rule.mdc

## Writing style

- **Tone**: professional, direct, instructive. Address the reader as "you" (second person).
- **Headings**: one H1 is provided by the frontmatter `title` — start body sections at H2 (`##`).
- **Code**: always specify a language fence (` ```yaml`, ` ```markdown`, ` ```text`). Use
  inline backticks for identifiers (`accessible_views`, `agents/rules/`).
- **Paragraphs**: keep them short; use `-` bullet lists for multiple items.

## File and frontmatter conventions

- Content is `.mdx`, organized by topic directory (e.g. `admin/ai/`, `docs/explore-analyze/`).
- The file path maps to the URL: `admin/ai/rules.mdx` → `/admin/ai/rules`.
- Every page starts with YAML frontmatter using `title` and `description`:

  ```mdx
  ---
  title: Rules
  description: One-sentence summary used for SEO and navigation previews.
  ---
  ```

- **Do not** add an H1 in the body — the `title` is the page heading.

## Navigation

Navigation is defined in `docs-mintlify/docs.json`. A new page only appears in the sidebar
once its path (without the `.mdx` extension) is added to the appropriate `group` in
`docs.json`. After adding a page, update `docs.json` and verify it is still valid JSON.

## Components

Mintlify provides these components (used throughout the docs):

- Callouts: `<Note>`, `<Warning>`, `<Info>`, `<Tip>`, `<Check>`
- `<Steps>` with nested `<Step title="...">` for sequential instructions
- `<CardGroup cols={2}>` with nested `<Card title="..." icon="..." href="...">`
- `<Tabs>` / `<Tab>`, `<Accordion>` / `<AccordionGroup>`, `<Frame>` for images

Content inside callouts and steps is plain MDX. Internal links are root-relative
(`/admin/ai/skills`), not file paths.

## Preview features

Every page documenting a feature that is in **preview** must open with a `<Warning>`
callout — placed right after the frontmatter, before the body — saying the feature is
in preview and that the user should reach out to the Cube support team to activate it
for their account:

```mdx
<Warning>

<Feature name> is currently in preview, and the user experience and file format may
still change. Reach out to the [Cube support team](/admin/account-billing/support)
to activate this feature for your account.

</Warning>
```

Adapt the "may still change" sentence per feature; the "in preview" + "reach out to
the Cube support team to activate it for your account" parts are required. Do not
expose internal feature-flag names in public docs.

## Images and screenshots

Wrap screenshots in `<Frame>` and store assets under `images/`. When a screenshot is
needed but not yet available, leave an MDX comment placeholder: `{/* TODO: screenshot — ... */}`.

## AI / agent docs structure

The agent configuration (code-first, developer-facing) lives under `admin/ai/`:
`rules.mdx`, `certified-queries.mdx`, `skills.mdx`, `memory-isolation.mdx`,
`multi-agent.mdx`, `bring-your-own-model.mdx`. The end-user chat experience
(explorer/viewer-facing) lives under `docs/explore-analyze/` (e.g. `analytics-chat.mdx`,
`skills.mdx`). Keep authoring docs in `admin/ai/` and usage docs in `docs/explore-analyze/`,
and cross-link the two.
