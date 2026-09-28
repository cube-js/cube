/* eslint-disable */
// Extractor: pulls the /api/chat/stream-chat-state path (+ its transitive schema
// closure) out of the AI Engineer OpenAPI spec into a standalone chat.yaml for
// the Mintlify API docs.
//
// The source spec lives in the (private) cubejs-enterprise repo and is NOT in
// this repo, so there is no hardcoded path — point the script at the spec via
// the SRC_SPEC env var (a CLI path arg is also accepted):
//
//   SRC_SPEC=/path/to/ai-engineer/open-api-spec.yaml node scripts/extract-chat.js
//   node scripts/extract-chat.js /path/to/ai-engineer/open-api-spec.yaml
//
// The spec is generated in cubejs-enterprise/packages/ai-engineer via
// `yarn generate:open-api:spec`.
const fs = require('fs');
const path = require('path');
const yaml = require('js-yaml');

const srcArg = process.argv[2] || process.env.SRC_SPEC;
if (!srcArg) {
  console.error(
    'No source spec provided. Set the SRC_SPEC env var (or pass a path arg):\n' +
      '  SRC_SPEC=/path/to/ai-engineer/open-api-spec.yaml node scripts/extract-chat.js\n' +
      '  node scripts/extract-chat.js /path/to/ai-engineer/open-api-spec.yaml\n\n' +
      'The spec is generated in cubejs-enterprise/packages/ai-engineer via\n' +
      '`yarn generate:open-api:spec` and is not committed to this repo.'
  );
  process.exit(1);
}
const SRC = path.resolve(srcArg);
if (!fs.existsSync(SRC)) {
  console.error(`Source spec not found: ${SRC}`);
  process.exit(1);
}
const OUT = path.join(__dirname, '..', 'api-reference', 'chat.yaml');

// Source path(s) to include. The public endpoint is served under
// .../agents/{agentId}, so strip the leading /api (the server URL holds it).
const SRC_PATHS = ['/api/chat/stream-chat-state'];
const TAG = 'Chat';
const METHODS = ['get', 'post', 'put', 'patch', 'delete'];

const src = yaml.load(fs.readFileSync(SRC, 'utf8'));

// 1. Filter + rewrite chat paths (strip leading /api -> server holds it),
//    retag to a clean name, and clean up operationIds for nice page slugs.
const paths = {};
for (const srcKey of SRC_PATHS) {
  const val = src.paths[srcKey];
  if (!val) {
    console.error(`Aborting: source path not found in spec: ${srcKey}`);
    process.exit(1);
  }
  const newKey = srcKey.replace(/^\/api/, '');
  for (const m of METHODS) {
    if (!val[m]) continue;
    val[m].tags = [TAG];
    if (typeof val[m].operationId === 'string') {
      val[m].operationId = val[m].operationId.replace(/^[^.]*\./, '');
    }
    // The public endpoint authenticates with an API key, not the JWT scheme
    // baked into the source spec. Override per-operation security.
    val[m].security = [{ apiKey: [] }];
  }
  paths[newKey] = val;
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
// Hard-fail rather than shipping chat.yaml with dangling $refs (broken docs).
if (missing.length) {
  console.error(
    'Aborting: referenced schemas not found in the source spec (broken $refs):\n  ' +
      missing.sort().join('\n  ') +
      '\nThe upstream spec likely renamed or removed these. Fix the mapping and re-run.'
  );
  process.exit(1);
}

// 3. Assemble output doc (sorted schemas for stable diff).
const sortedSchemas = {};
Object.keys(schemas).sort().forEach((k) => { sortedSchemas[k] = schemas[k]; });

const out = {
  openapi: '3.1.0',
  info: {
    title: 'Cube Cloud Chat API',
    version: '1.0.0',
    description:
      'Real-time streaming conversations with Cube Cloud AI agents for analytics and\n' +
      'data exploration. Copy the exact Chat API URL from your agent settings\n' +
      '(Admin → Agents → Chat API URL).',
  },
  servers: [
    {
      url: 'https://ai.{cloudRegion}.cubecloud.dev/api/v1/public/{accountName}/agents/{agentId}',
      description:
        'Chat API base URL. Copy the exact URL from your agent settings ' +
        '(Admin → Agents → Chat API URL).',
      variables: {
        cloudRegion: { default: 'gcp-us-central1', description: 'Cloud region identifier' },
        accountName: { default: 'your-account', description: 'Your Cube Cloud account/tenant name' },
        agentId: { default: '1', description: 'AI agent identifier (Admin → Agents)' },
      },
    },
  ],
  security: [{ apiKey: [] }],
  tags: [{ name: TAG }],
  paths,
  components: {
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
console.log('paths:', Object.keys(paths).length, '| schemas:', Object.keys(schemas).length);

// 4. Emit nav fragment (pages in file order).
const pages = [];
for (const [p, val] of Object.entries(paths)) {
  for (const m of METHODS) {
    if (!val[m]) continue;
    pages.push(`${m.toUpperCase()} ${p}`);
  }
}
const group = { group: TAG, openapi: '/api-reference/chat.yaml', pages };
console.log('\n=== NAV GROUP ===');
console.log(JSON.stringify(group, null, 2));
