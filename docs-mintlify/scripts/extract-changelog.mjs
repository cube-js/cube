/* eslint-disable */
// Extractor: turns the hand-written "Keep a Changelog"-style CHANGELOG.md of the
// platform client package (in the private cube cloud monorepo) into the Mintlify
// changelog page api-reference/changelog.mdx (a vertical <Update> timeline with
// an RSS feed). Run it in the same step where you re-extract the OpenAPI spec
// (extract-api.mjs) so the API reference and its changelog ship together.
//
// The source CHANGELOG.md is NOT in this repo, so there is no hardcoded path —
// point the script at it via the SRC_CHANGELOG env var (a CLI path arg also works):
//
//   SRC_CHANGELOG=/path/to/platform-client/CHANGELOG.md node scripts/extract-changelog.mjs
//   node scripts/extract-changelog.mjs /path/to/platform-client/CHANGELOG.md
//
// Expected source format (Keep a Changelog, https://keepachangelog.com):
//
//   # Changelog
//
//   ## [1.2.0] - 2026-06-06
//   ### Added
//   - New workbook publish endpoint.
//   ### Fixed
//   - Corrected report refresh response.
//
//   ## [1.1.0] - 2026-05-20
//   ...
//
// Each `## [version] - date` release becomes one <Update> block; its `### Added`
// / `### Fixed` / ... subsection names become the entry's tags.
import fs from 'fs';
import path from 'path';

// Load docs-mintlify/.env (SRC_CHANGELOG etc.) if present. A real env var or CLI
// arg still wins, since loadEnvFile does not clobber already-set process.env keys.
const envPath = path.join(import.meta.dirname, '..', '.env');
if (fs.existsSync(envPath)) process.loadEnvFile(envPath);

const srcArg = process.argv[2] || process.env.SRC_CHANGELOG;
if (!srcArg) {
  console.error(
    'No source changelog provided. Set the SRC_CHANGELOG env var (or pass a path arg):\n' +
      '  SRC_CHANGELOG=/path/to/platform-client/CHANGELOG.md node scripts/extract-changelog.mjs\n' +
      '  node scripts/extract-changelog.mjs /path/to/platform-client/CHANGELOG.md\n\n' +
      'The CHANGELOG.md lives in the platform client package in the cube cloud\n' +
      'monorepo and is not committed to this repo.'
  );
  process.exit(1);
}
const SRC = path.resolve(srcArg);
if (!fs.existsSync(SRC)) {
  console.error(`Source changelog not found: ${SRC}`);
  process.exit(1);
}
const OUT = path.join(import.meta.dirname, '..', 'api-reference', 'changelog.mdx');

const raw = fs.readFileSync(SRC, 'utf8');
const lines = raw.split(/\r?\n/);

// Parse `## [version] - date` (or `## version - date`) release headings and the
// lines that follow up to the next release heading.
const RELEASE_RE = /^##\s+\[?([^\]\s]+)\]?\s*[-–—]\s*(\d{4}-\d{2}-\d{2})\s*$/;
const releases = [];
let current = null;
for (const line of lines) {
  const m = line.match(RELEASE_RE);
  if (m) {
    current = { version: m[1], date: m[2], body: [] };
    releases.push(current);
  } else if (current) {
    current.body.push(line);
  }
}

if (!releases.length) {
  console.error(
    'Aborting: no releases parsed from the source changelog.\n' +
      'Expected headings like `## [1.2.0] - 2026-06-06`. Check the source format.'
  );
  process.exit(1);
}

// Collect `### Section` names within a release body to use as tags.
function tagsFor(body) {
  const tags = [];
  for (const line of body) {
    const m = line.match(/^###\s+(.+?)\s*$/);
    if (m && !tags.includes(m[1])) tags.push(m[1]);
  }
  return tags;
}

// Trim leading/trailing blank lines and indent body two spaces so it nests
// cleanly inside the <Update> JSX block.
function indentBody(body) {
  const trimmed = [...body];
  while (trimmed.length && trimmed[0].trim() === '') trimmed.shift();
  while (trimmed.length && trimmed[trimmed.length - 1].trim() === '') trimmed.pop();
  return trimmed.map((l) => (l.trim() === '' ? '' : `  ${l}`)).join('\n');
}

const blocks = releases.map((r) => {
  const tags = tagsFor(r.body);
  const tagsAttr = tags.length ? ` tags={${JSON.stringify(tags)}}` : '';
  return (
    `<Update label="${r.date}" description="v${r.version}"${tagsAttr}>\n` +
    `${indentBody(r.body)}\n` +
    `</Update>`
  );
});

const out =
  `---\n` +
  `title: Changelog\n` +
  `description: Release notes for the Cube Cloud platform client and management API.\n` +
  `rss: true\n` +
  `---\n\n` +
  `{/* GENERATED FILE — do not edit by hand. */}\n` +
  `{/* Run scripts/extract-changelog.js against the platform client CHANGELOG.md. */}\n\n` +
  blocks.join('\n\n') +
  `\n`;

fs.writeFileSync(OUT, out);
console.log('Wrote', OUT);
console.log('releases:', releases.length, '| latest:', releases[0].version, releases[0].date);
