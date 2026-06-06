/* eslint-disable */
// Extractor: turns the Console Server *public* OpenAPI spec into a standalone
// api.yaml covering the whole v1 REST surface for the Mintlify API docs. Unlike
// the old deployments-only extractor, this includes every /api/v1 endpoint the
// public spec exposes (deployments and everything scoped to them, plus account,
// embed, AI, and workspace areas) and auto-discovers tags, so new public
// endpoints show up without editing this script.
//
// SCIM (/api/scim/v2) is intentionally NOT included: it authenticates with a
// Bearer token rather than the Api-Key scheme used here, and its docs are
// hand-curated in api-reference/scim.yaml.
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
  if (paths[newKey]) {
    console.error(`Aborting: path collision after normalization: ${newKey} (from ${key}).`);
    process.exit(1);
  }
  for (const m of METHODS) {
    if (!val[m]) continue;
    if (Array.isArray(val[m].tags)) {
      val[m].tags = val[m].tags.map(cleanTag);
    }
    // strip "XxxController." prefix from operationId for clean page slugs
    if (typeof val[m].operationId === 'string') {
      val[m].operationId = val[m].operationId.replace(/^[^.]*\./, '');
    }
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
  security: [{ apiKey: [] }],
  tags: orderedTags.map((t) => ({ name: t })),
  paths,
  components: {
    // The public API authenticates REST clients with an API key sent as
    // `Authorization: Api-Key <token>`. The source spec defines a bearer/JWT
    // scheme that does not reflect the primary runtime auth, so we override it.
    securitySchemes: {
      apiKey: {
        type: 'apiKey',
        in: 'header',
        name: 'Authorization',
        description: 'API key authentication. Send `Authorization: Api-Key <YOUR_API_KEY>`.',
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
