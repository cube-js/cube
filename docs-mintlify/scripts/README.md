# Scripts

## `upload-asset.sh`

Uploads a static asset to the `cube-dev-websites-shared` S3 bucket and prints
the resulting `https://static.cube.dev/<key>` URL (also copied to clipboard on
macOS). Use this for images and other binaries referenced from `.mdx` / `.md`
files in the Mintlify docs.

### One-time setup

1. Install the AWS CLI:

   ```bash
   brew install awscli
   ```

2. Configure a local profile (name it `cube-static` so the script picks it up
   automatically; or use any name and export `AWS_PROFILE` before running):

   ```bash
   aws configure --profile cube-static
   # AWS Access Key ID:     <your key>
   # AWS Secret Access Key: <your secret>
   # Default region name:   us-west-2
   # Default output format: json
   ```

   You need credentials that can `s3:PutObject` and `s3:HeadObject` on
   `cube-dev-websites-shared`. Ask whoever manages Cube's AWS account if you
   don't have them yet.

3. Verify:

   ```bash
   aws sts get-caller-identity --profile cube-static
   aws s3 ls s3://cube-dev-websites-shared/icons/ --profile cube-static | head
   ```

### Usage

Run from the `docs-mintlify/` directory:

```bash
./scripts/upload-asset.sh <local-file> <dest-key> [--force]
```

Examples:

```bash
./scripts/upload-asset.sh ./snowflake.svg     icons/snowflake.svg
./scripts/upload-asset.sh ./architecture.png  docs/getting-started/architecture.png
./scripts/upload-asset.sh ./flow.svg          diagrams/pre-aggregations-flow.svg
```

Output:

```
→ bucket:       s3://cube-dev-websites-shared/icons/snowflake.svg
→ region:       us-west-2
→ profile:      cube-static
→ content-type: image/svg+xml
→ cache:        public, max-age=31536000, immutable

✓ uploaded
  https://static.cube.dev/icons/snowflake.svg
  (copied to clipboard)
```

Paste the URL into the relevant `.mdx` file and commit.

### Path conventions

Assets are grouped by content domain so the same asset can be reused across
pages. Use kebab-case filenames.

| Prefix                            | Purpose                                            |
| --------------------------------- | -------------------------------------------------- |
| `icons/<slug>.svg`                | Provider / integration / vendor logos for `<Card>` |
| `icons/<slug>-light.svg`          | Logo, light variant (use on dark backgrounds)      |
| `icons/<slug>-dark.svg`           | Logo, dark variant (use on light backgrounds)      |
| `docs/<section>/<slug>/<file>`    | Screenshots & images for a specific docs page      |
| `diagrams/<slug>.svg`             | Architecture / flow diagrams                       |
| `recipes/<slug>/<file>`           | Recipe-specific screenshots                        |

For provider logos, prefer SVG. For UI screenshots, prefer PNG (or WebP for
larger images). Compress before uploading — the bucket is cached aggressively.

### Immutability

Paths are **immutable by convention**. The script refuses to overwrite an
existing key. If an asset needs to change:

1. Upload a new key with a version suffix: `snowflake-v2.svg`.
2. Update the `.mdx` reference in the same PR.

This keeps `Cache-Control: public, max-age=31536000, immutable` safe and makes
rollbacks trivial (just revert the Markdown change).

If you genuinely need to overwrite (e.g. you uploaded a corrupt file in the
same session and the CDN hasn't cached it yet), pass `--force`:

```bash
./scripts/upload-asset.sh ./fixed.svg icons/snowflake.svg --force
```

Avoid `--force` for anything already live.
