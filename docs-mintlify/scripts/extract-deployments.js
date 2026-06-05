/* eslint-disable */
// One-off extractor: pulls the /api/v1/deployments subtree (paths + transitive
// schema closure) out of the enterprise public OpenAPI spec into a standalone
// deployments.yaml for the Mintlify API docs.
const fs = require('fs');
const path = require('path');
const yaml = require('js-yaml');

const SRC = path.join(
  process.env.HOME,
  'code/cubejs-enterprise/packages/console-server/open-api-spec-public-v3.1.yaml'
);
const OUT = path.join(__dirname, '..', 'api-reference', 'deployments.yaml');

const TAG_MAP = {
  'Deployments Public': 'Deployments',
  'Deployment Environment Public': 'Environments',
  'Agents Public': 'Agents',
  'Folders Public': 'Folders',
  'Reports Public': 'Reports',
  'Workbooks Public': 'Workbooks',
  'Workspace Public': 'Workspace',
};
// Allowlist of areas to include (also controls tag + nav order).
const TAG_ORDER = ['Deployments', 'Environments', 'Folders', 'Reports', 'Workbooks'];
const METHODS = ['get', 'post', 'put', 'patch', 'delete'];

const src = yaml.load(fs.readFileSync(SRC, 'utf8'));

// 1. Filter + rewrite deployments paths (strip leading /api -> server holds it).
const paths = {};
for (const [key, val] of Object.entries(src.paths)) {
  if (!key.startsWith('/api/v1/deployments')) continue;
  // skip whole paths whose area is not in the allowlist (e.g. Agents, Workspace)
  const firstOp = METHODS.map((m) => val[m]).find(Boolean);
  const cleanTag = firstOp && firstOp.tags && (TAG_MAP[firstOp.tags[0]] || firstOp.tags[0]);
  if (!TAG_ORDER.includes(cleanTag)) continue;
  const newKey = key.replace(/^\/api/, '');
  for (const m of METHODS) {
    if (!val[m]) continue;
    // retag operations to clean names
    if (Array.isArray(val[m].tags)) {
      val[m].tags = val[m].tags.map((t) => TAG_MAP[t] || t);
    }
    // strip "XxxController." prefix from operationId for clean page slugs
    if (typeof val[m].operationId === 'string') {
      val[m].operationId = val[m].operationId.replace(/^[^.]*\./, '');
    }
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
const queue = [...wanted];
while (queue.length) {
  const name = queue.shift();
  if (schemas[name]) continue;
  const def = src.components.schemas[name];
  if (!def) { console.warn('MISSING schema:', name); continue; }
  schemas[name] = def;
  const sub = new Set();
  collectRefs(def, sub);
  for (const s of sub) if (!schemas[s]) queue.push(s);
}

// 3. Assemble output doc (sorted schemas for stable diff).
const sortedSchemas = {};
Object.keys(schemas).sort().forEach((k) => { sortedSchemas[k] = schemas[k]; });

const out = {
  openapi: '3.1.0',
  info: {
    title: 'Cube Cloud Deployments API',
    version: '1.0.0',
    description:
      'Manage Cube Cloud deployments and everything scoped to them — environments and\n' +
      'API tokens, folders, reports, and workbooks with their dashboards.',
  },
  servers: [
    {
      url: 'https://{tenant}.cubecloud.dev/api',
      description: 'Cube Cloud API base URL. Replace the whole host if you use a custom domain.',
      variables: { tenant: { default: 'your-tenant', description: 'Your Cube Cloud tenant subdomain' } },
    },
  ],
  security: [{ jwtAuth: [] }],
  tags: TAG_ORDER.map((t) => ({ name: t })),
  paths,
  components: {
    securitySchemes: { jwtAuth: src.components.securitySchemes.jwtAuth },
    schemas: sortedSchemas,
  },
};

fs.writeFileSync(OUT, yaml.dump(out, { lineWidth: 100, noRefs: true }));
console.log('Wrote', OUT);
console.log('paths:', Object.keys(paths).length, '| schemas:', Object.keys(schemas).length);

// 4. Emit nav fragment (pages grouped by clean tag, in file order) + index rows.
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
const groups = TAG_ORDER.filter((t) => byTag[t]).map((t) => ({
  group: t,
  openapi: '/api-reference/deployments.yaml',
  pages: byTag[t],
}));
console.log('\n=== NAV GROUPS ===');
console.log(JSON.stringify(groups, null, 2));
console.log('\n=== INDEX ROWS (entity | resource | version) ===');
for (const t of TAG_ORDER) {
  if (firstPathForTag[t]) console.log(`${t} | ${firstPathForTag[t]} | v1`);
}
