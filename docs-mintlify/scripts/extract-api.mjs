/* eslint-disable */
// Extractor: turns the Console Server *public* OpenAPI spec into a standalone
// api.yaml covering the whole v1 REST surface for the Mintlify API docs. Unlike
// the old deployments-only extractor, this includes every /api/v1 endpoint the
// public spec exposes (deployments and everything scoped to them, plus account,
// embed, AI, and workspace areas) and auto-discovers tags, so new public
// endpoints show up without editing this script.
//
// SCIM (/api/scim/v2) is intentionally NOT included: its docs are hand-curated
// in api-reference/scim.yaml. (Both the REST API and SCIM authenticate with a
// Bearer token; see the securityScheme override below.)
//
// The source spec lives in the (private) cubejs-enterprise repo and is NOT in
// this repo, so there is no hardcoded path — point the script at the spec via
// the SRC_SPEC env var (a CLI path arg is also accepted), or set it in
// docs-mintlify/.env:
//
//   SRC_SPEC=/path/to/open-api-spec-public-v3.1.yaml node scripts/extract-api.mjs
//   node scripts/extract-api.mjs /path/to/open-api-spec-public-v3.1.yaml
//
// The spec is generated in cubejs-enterprise/packages/console-server via
// `yarn generate:open-api:spec-public`.
import fs from 'fs';
import path from 'path';
import yaml from 'js-yaml';

// Load docs-mintlify/.env (SRC_SPEC etc.) if present. A real env var or CLI arg
// still wins, since loadEnvFile does not clobber already-set process.env keys.
const envPath = path.join(import.meta.dirname, '..', '.env');
if (fs.existsSync(envPath)) process.loadEnvFile(envPath);

const srcArg = process.argv[2] || process.env.SRC_SPEC;
if (!srcArg) {
  console.error(
    'No source spec provided. Set the SRC_SPEC env var (or pass a path arg):\n' +
      '  SRC_SPEC=/path/to/open-api-spec-public-v3.1.yaml node scripts/extract-api.mjs\n' +
      '  node scripts/extract-api.mjs /path/to/open-api-spec-public-v3.1.yaml\n\n' +
      'The spec is generated in cubejs-enterprise/packages/console-server via\n' +
      '`yarn generate:open-api:spec-public` and is not committed to this repo.'
  );
  process.exit(1);
}
const SRC = path.resolve(srcArg);
if (!fs.existsSync(SRC)) {
  console.error(`Source spec not found: ${SRC}`);
  process.exit(1);
}
const OUT = path.join(import.meta.dirname, '..', 'api-reference', 'api.yaml');

// Only the v1 REST API. SCIM lives in the hand-curated scim.yaml (different auth).
const INCLUDE_PREFIX = '/api/v1/';
const METHODS = ['get', 'post', 'put', 'patch', 'delete'];

// Operations to hide from the public docs even though the source spec exposes
// them — e.g. stray/internal admin routes that surface a single, incomplete
// endpoint. Listed as "METHOD /path" using the normalized path (no /api prefix,
// no trailing slash). Kept here so re-pulling an updated upstream spec does NOT
// resurface them. If a path's only operations are excluded, the whole path (and
// its now-empty nav group) is dropped automatically.
const EXCLUDE_OPERATIONS = new Set([
  // Stray/incomplete admin routes.
  'DELETE /v1/groups/{id}',
  'GET /v1/user-groups',
  // Account-level / internal admin APIs kept out of the public docs.
  'GET /v1/deployments/{deploymentId}/agent-skills',
  'GET /v1/deployments/{deploymentId}/agents',
  'POST /v1/meta',
  'GET /v1/users',
  'GET /v1/users/embed-theme',
  'GET /v1/users/me',
  'DELETE /v1/user-attributes/{id}',
  'GET /v1/resource-policies',
  'PUT /v1/resource-policies/group',
  'PUT /v1/resource-policies/user',
  'GET /v1/app-theme',
  'GET /v1/ai-engineer/active-region',
  'GET /v1/ai-engineer/settings',
  // Report folders listing — not part of the public docs surface.
  'GET /v1/deployments/{deploymentId}/report-folders',
]);

// Explicit display names for tags whose auto-cleaned form would be unclear or
// collide. Everything else is cleaned by cleanTag() below.
const TAG_MAP = {
  'Deployment Environment Public': 'Environments',
  'Embed Tenant Admin Public': 'Embed Tenants',
};
// Preferred nav order. Tags not listed here are appended alphabetically, so the
// docs stay complete even when the upstream spec adds new areas.
const TAG_ORDER = [
  'Deployments', 'Environments', 'Folders', 'Reports', 'Workbooks',
  'Notifications', 'Workspace', 'Agents', 'Metadata',
  'Users', 'Groups', 'User Groups', 'User Attributes', 'Resource Policies',
  'App Theme', 'AI Engineer', 'Embed', 'Embed Tenants',
];

// Mintlify renders the OpenAPI operation `description` as a plain-text node — it
// does NOT process Markdown or HTML there, so `**bold**` and `` `code` `` show up
// literally on the page (verified by headless-rendering with Mintlify CLI 4.2.x).
// The fix is to move the prose into the `x-mint.content` extension instead, which
// Mintlify DOES render as MDX (so bold/italic/code render), and drop the plain
// `description` so it isn't also shown unformatted. See applyDescription() below.
// (Parameter/schema descriptions render fine via a different component, so they
// are left alone.)

// x-mint.content is MDX, where `{...}` is a JS expression — unescaped prose braces
// (e.g. `Copy of {original name}`) break the page. Escape braces OUTSIDE inline
// code spans (inside backticks they're literal and must stay as-is). `**bold**`
// and `` `code` `` are valid MDX and pass through untouched.
function toMintContent(s) {
  return s
    .split(/(`[^`]*`)/) // keep code spans as their own (odd-index) segments
    .map((seg, i) => (i % 2 === 1 ? seg : seg.replace(/[{}]/g, (c) => '\\' + c)))
    .join('');
}

// Move an operation's Markdown description into x-mint.content (rendered as MDX)
// and remove the plain `description` so it is not also rendered unformatted.
//
// NB: do NOT also set x-mint.metadata.description — Mintlify injects that into the
// generated page's MDX frontmatter, where prose containing `"`, `:` or `{` breaks
// the YAML parse (500 / "multiline key may not be an implicit key"). The page just
// loses its `<meta name="description">`, which is an acceptable trade for not
// crashing the page.
function applyDescription(op) {
  if (typeof op.description !== 'string') return;
  op['x-mint'] = { content: toMintContent(op.description) };
  delete op.description;
}

// Strip the " Public" suffix the source appends to every tag and normalize a few
// acronyms; TAG_MAP overrides win.
function cleanTag(raw) {
  if (TAG_MAP[raw]) return TAG_MAP[raw];
  return raw
    .replace(/\s+Public$/, '')
    .replace(/\bScim\b/g, 'SCIM')
    .replace(/\bAi\b/g, 'AI');
}

const src = yaml.load(fs.readFileSync(SRC, 'utf8'));

// 1. Filter to v1 REST paths, normalize keys (strip leading /api; drop trailing
//    slash — a trailing slash breaks Mintlify dev), and clean tags + operationIds.
const paths = {};
for (const [key, val] of Object.entries(src.paths)) {
  if (!key.startsWith(INCLUDE_PREFIX)) continue;
  let newKey = key.replace(/^\/api/, '');
  if (newKey.length > 1) newKey = newKey.replace(/\/$/, ''); // drop trailing slash
  let kept = 0;
  for (const m of METHODS) {
    if (!val[m]) continue;
    // Drop explicitly hidden operations before they reach the spec or nav.
    if (EXCLUDE_OPERATIONS.has(`${m.toUpperCase()} ${newKey}`)) {
      delete val[m];
      continue;
    }
    kept++;
    if (Array.isArray(val[m].tags)) {
      val[m].tags = val[m].tags.map(cleanTag);
    }
    // Mintlify shows the operation description as plain text, so move the prose
    // into x-mint.content (rendered as MDX) and keep a plain copy for SEO.
    applyDescription(val[m]);
    // strip "XxxController." prefix from operationId for clean page slugs
    if (typeof val[m].operationId === 'string') {
      val[m].operationId = val[m].operationId.replace(/^[^.]*\./, '');
    }
  }
  if (!kept) continue; // every operation on this path was excluded
  if (paths[newKey]) {
    console.error(`Aborting: path collision after normalization: ${newKey} (from ${key}).`);
    process.exit(1);
  }
  paths[newKey] = val;
}

if (!Object.keys(paths).length) {
  console.error(`Aborting: no paths matched ${INCLUDE_PREFIX}. Check the source spec.`);
  process.exit(1);
}

// 2. Transitive $ref schema closure.
function collectRefs(node, acc) {
  if (Array.isArray(node)) { node.forEach((n) => collectRefs(n, acc)); return; }
  if (node && typeof node === 'object') {
    for (const [k, v] of Object.entries(node)) {
      if (k === '$ref' && typeof v === 'string') {
        const m = v.match(/^#\/components\/schemas\/(.+)$/);
        if (m) acc.add(m[1]);
      } else collectRefs(v, acc);
    }
  }
}
const wanted = new Set();
collectRefs(paths, wanted);
const schemas = {};
const missing = [];
const queue = [...wanted];
while (queue.length) {
  const name = queue.shift();
  if (schemas[name]) continue;
  const def = src.components.schemas[name];
  if (!def) { missing.push(name); continue; }
  schemas[name] = def;
  const sub = new Set();
  collectRefs(def, sub);
  for (const s of sub) if (!schemas[s]) queue.push(s);
}
// Hard-fail rather than shipping api.yaml with dangling $refs (broken docs).
if (missing.length) {
  console.error(
    'Aborting: referenced schemas not found in the source spec (broken $refs):\n  ' +
      missing.sort().join('\n  ') +
      '\nThe upstream spec likely renamed or removed these. Fix the mapping and re-run.'
  );
  process.exit(1);
}

// 3. Determine tag set + order (preferred order first, then any extras A–Z).
const presentTags = new Set();
for (const val of Object.values(paths)) {
  for (const m of METHODS) {
    if (val[m] && Array.isArray(val[m].tags) && val[m].tags[0]) presentTags.add(val[m].tags[0]);
  }
}
const extras = [...presentTags].filter((t) => !TAG_ORDER.includes(t)).sort();
if (extras.length) {
  console.log('Note: tags not in TAG_ORDER (appended A–Z):', extras.join(', '));
}
const orderedTags = [...TAG_ORDER.filter((t) => presentTags.has(t)), ...extras];

// 4. Assemble output doc (sorted schemas for stable diff).
const sortedSchemas = {};
Object.keys(schemas).sort().forEach((k) => { sortedSchemas[k] = schemas[k]; });

const out = {
  openapi: '3.1.0',
  info: {
    title: 'Cube Cloud REST API',
    version: '1.0.0',
    description:
      'Programmatically manage Cube Cloud: deployments and everything scoped to them\n' +
      '(environments, folders, reports, workbooks, notifications, workspace, and agents),\n' +
      'plus account-level users, groups, policies, embedding, and AI settings.',
  },
  servers: [
    {
      url: 'https://{tenant}.cubecloud.dev/api',
      description: 'Cube Cloud API base URL. Replace the whole host if you use a custom domain.',
      variables: { tenant: { default: 'your-tenant', description: 'Your Cube Cloud tenant subdomain' } },
    },
  ],
  security: [{ bearerAuth: [] }],
  tags: orderedTags.map((t) => ({ name: t })),
  paths,
  components: {
    // The public REST API authenticates with a token sent as
    // `Authorization: Bearer <token>` (an API key or an OAuth access token). The
    // source spec's scheme does not reflect the primary runtime auth, so override.
    securitySchemes: {
      bearerAuth: {
        type: 'http',
        scheme: 'bearer',
        description: 'Token authentication. Send `Authorization: Bearer <YOUR_TOKEN>`.',
      },
    },
    schemas: sortedSchemas,
  },
};

fs.writeFileSync(OUT, yaml.dump(out, { lineWidth: 100, noRefs: true }));
console.log('Wrote', OUT);
console.log('paths:', Object.keys(paths).length, '| schemas:', Object.keys(schemas).length, '| tags:', orderedTags.length);

// 5. Emit nav fragment (groups in tag order; pages in source order within a tag)
//    + index rows for the introduction table.
const byTag = {};
const firstPathForTag = {};
for (const [p, val] of Object.entries(paths)) {
  for (const m of METHODS) {
    if (!val[m]) continue;
    const tag = (val[m].tags && val[m].tags[0]) || 'Other';
    (byTag[tag] = byTag[tag] || []).push(`${m.toUpperCase()} ${p}`);
    if (!firstPathForTag[tag]) firstPathForTag[tag] = p;
  }
}
const groups = orderedTags.filter((t) => byTag[t]).map((t) => ({
  group: t,
  openapi: '/api-reference/api.yaml',
  pages: byTag[t],
}));
console.log('\n=== NAV GROUPS (paste into docs.json API tab) ===');
console.log(JSON.stringify(groups, null, 2));
console.log('\n=== INDEX ROWS (entity | resource | version) ===');
for (const t of orderedTags) {
  if (firstPathForTag[t]) console.log(`${t} | ${firstPathForTag[t]} | v1`);
}
