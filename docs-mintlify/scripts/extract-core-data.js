/* eslint-disable */
// Extractor: builds the Core Data REST (JSON) API reference for the Mintlify docs
// from the OSS gateway OpenAPI spec.
//
// Unlike the Platform API specs (which live in the private cubejs-enterprise
// repo), the source for the Core Data REST API is in THIS monorepo at
// packages/cubejs-api-gateway/openspec.yml, so the default path points there.
// Override with the SRC_SPEC env var or a CLI path arg if needed:
//
//   node scripts/extract-core-data.js
//   SRC_SPEC=/path/to/openspec.yml node scripts/extract-core-data.js
//
// Scope (minimal): the upstream spec currently documents POST /v1/load and
// GET /v1/meta. The remaining REST endpoints (sql, convert-query, cubesql,
// running-query, pre-aggregations/jobs, metadata, health) stay as prose in
// reference/core-data-apis/rest-api/reference.mdx until the upstream spec covers
// them — add them to PATHS below as the spec grows.
const fs = require('fs');
const path = require('path');
const yaml = require('js-yaml');

const DEFAULT_SRC = path.join(
  __dirname, '..', '..', 'packages', 'cubejs-api-gateway', 'openspec.yml'
);
const srcArg = process.argv[2] || process.env.SRC_SPEC || DEFAULT_SRC;
const SRC = path.resolve(srcArg);
if (!fs.existsSync(SRC)) {
  console.error(`Source spec not found: ${SRC}`);
  process.exit(1);
}
const OUT = path.join(__dirname, '..', 'api-reference', 'core-data.yaml');

// Allowlist of source paths to extract from the upstream spec.
const PATHS = ['/v1/load', '/v1/meta'];
const TAG = 'Data';
const METHODS = ['get', 'post', 'put', 'patch', 'delete'];

// Cleaner page titles than the upstream summaries (keyed by "METHOD path").
const SUMMARY_OVERRIDES = {
  'POST /v1/load': 'JSON query',
};

// Final output order of paths (extracted + hand-authored).
const PATH_ORDER = ['/v1/load', '/v1/cubesql', '/v1/meta'];

// Endpoints not yet in the upstream openspec.yml, hand-authored here. Keep them
// in sync with reference/core-data-apis/rest-api/reference.mdx; remove an entry
// once the upstream spec covers it (then add its path to PATHS instead).
const EXTRA_PATHS = {
  '/v1/cubesql': {
    post: {
      operationId: 'cubesqlV1',
      summary: 'SQL query',
      description:
        'Run a SQL query against the Cube [SQL API](/reference/core-data-apis/sql-api) ' +
        'and stream the results. The response is newline-delimited JSON: the first line ' +
        'carries the `schema` (column names and types) and optionally `lastRefreshTime`; ' +
        'each subsequent line carries a `data` chunk with one or more result rows.',
      requestBody: {
        required: true,
        content: {
          'application/json': {
            schema: {
              type: 'object',
              required: ['query'],
              properties: {
                query: { type: 'string', description: 'The SQL query to run.' },
                timezone: {
                  type: 'string',
                  description:
                    'Time zone for the query in TZ database name format, e.g. America/Los_Angeles.',
                },
                cache: {
                  type: 'string',
                  enum: ['stale-if-slow', 'stale-while-revalidate', 'must-revalidate', 'no-cache'],
                  default: 'stale-if-slow',
                  description:
                    'In-memory cache strategy (see ' +
                    '[Cache control](/reference/core-data-apis/rest-api#cache-control)):\n\n' +
                    '- `stale-if-slow` (default): if refresh keys are up to date, return the cached ' +
                    'value; if expired, fetch fresh data but fall back to the stale value when the ' +
                    'source query is slow.\n' +
                    '- `stale-while-revalidate`: if expired, return stale data immediately and ' +
                    'refresh the cache in the background.\n' +
                    '- `must-revalidate`: if expired, always wait for fresh data from the source, ' +
                    'even if slow.\n' +
                    '- `no-cache`: skip refresh-key checks and always query the data source.',
                },
              },
            },
          },
        },
      },
      responses: {
        '200': {
          description:
            'Newline-delimited JSON stream. The first line carries `schema` (and optionally ' +
            '`lastRefreshTime`); subsequent lines carry `data` chunks.',
          content: {
            'application/json': {
              schema: {
                type: 'object',
                properties: {
                  schema: {
                    type: 'array',
                    items: {
                      type: 'object',
                      properties: {
                        name: { type: 'string' },
                        column_type: { type: 'string' },
                      },
                    },
                  },
                  lastRefreshTime: { type: 'string', format: 'date-time' },
                  data: { type: 'array', items: { type: 'array', items: {} } },
                },
              },
              examples: {
                schemaLine: {
                  summary: 'First line (schema)',
                  value: {
                    schema: [{ name: 'value', column_type: 'Int64' }],
                    lastRefreshTime: '2025-01-13T12:00:00.000Z',
                  },
                },
                dataLine: { summary: 'Data chunk', value: { data: [['123']] } },
              },
            },
          },
        },
      },
    },
  },
};

const src = yaml.load(fs.readFileSync(SRC, 'utf8'));

// 1. Collect extracted + hand-authored paths, retag, override auth and summaries.
const collected = {};
for (const key of PATHS) {
  const val = src.paths[key];
  if (!val) {
    console.error(`Aborting: source path not found in spec: ${key}`);
    process.exit(1);
  }
  collected[key] = val;
}
Object.assign(collected, EXTRA_PATHS);

for (const [key, val] of Object.entries(collected)) {
  for (const m of METHODS) {
    if (!val[m]) continue;
    val[m].tags = [TAG];
    // The data API authenticates with a Cube API token sent directly in the
    // Authorization header (no "Bearer" prefix), not the http-bearer scheme the
    // upstream spec declares. Pin each operation to the apiToken scheme.
    val[m].security = [{ apiToken: [] }];
    const override = SUMMARY_OVERRIDES[`${m.toUpperCase()} ${key}`];
    if (override) val[m].summary = override;
  }
}

// Assemble in the desired output order.
const paths = {};
for (const key of PATH_ORDER) {
  if (collected[key]) paths[key] = collected[key];
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
// Hard-fail rather than shipping core-data.yaml with dangling $refs.
if (missing.length) {
  console.error(
    'Aborting: referenced schemas not found in the source spec (broken $refs):\n  ' +
      missing.sort().join('\n  ')
  );
  process.exit(1);
}

// 3. Assemble output doc (sorted schemas for stable diff).
const sortedSchemas = {};
Object.keys(schemas).sort().forEach((k) => { sortedSchemas[k] = schemas[k]; });

const out = {
  openapi: '3.0.0',
  info: {
    title: 'Cube Core Data REST (JSON) API',
    version: '1.0.0',
    description:
      'Deliver data from Cube over HTTP. Run queries with `POST /v1/load` and\n' +
      'retrieve the data model with `GET /v1/meta`. The `{base_path}` segment\n' +
      '(`/cubejs-api` by default) is configurable.',
  },
  servers: [
    {
      url: 'https://{deployment}.{region}.cubecloudapp.dev/cubejs-api',
      description:
        'Cube Cloud deployment REST API. The host includes your deployment slug and ' +
        'region; copy the exact URL from your deployment\'s API settings. The ' +
        '/cubejs-api base path is configurable.',
      variables: {
        deployment: { default: 'your-deployment', description: 'Your Cube Cloud deployment slug' },
        region: {
          default: 'aws-us-east-1',
          description: 'Cloud region of your deployment, e.g. aws-us-east-1, aws-eu-central-1, gcp-us-central1',
        },
      },
    },
    {
      url: 'http://localhost:4000/cubejs-api',
      description: 'Self-hosted Cube (default port and base path).',
    },
  ],
  security: [{ apiToken: [] }],
  tags: [{ name: TAG }],
  paths,
  components: {
    securitySchemes: {
      apiToken: {
        type: 'apiKey',
        in: 'header',
        name: 'Authorization',
        description:
          'Cube API token (a JWT). Sent directly in the Authorization header, ' +
          'e.g. `Authorization: <CUBE_API_TOKEN>` (no "Bearer" prefix).',
      },
    },
    schemas: sortedSchemas,
  },
};

fs.writeFileSync(OUT, yaml.dump(out, { lineWidth: 100, noRefs: true }));
console.log('Wrote', OUT);
console.log('paths:', Object.keys(paths).length, '| schemas:', Object.keys(schemas).length);

// 4. Emit nav fragment (pages in allowlist order).
const pages = [];
for (const p of Object.keys(paths)) {
  for (const m of METHODS) {
    if (!paths[p][m]) continue;
    pages.push(`${m.toUpperCase()} ${p}`);
  }
}
const group = { group: 'REST (JSON) API', openapi: '/api-reference/core-data.yaml', pages };
console.log('\n=== NAV PAGES ===');
console.log(JSON.stringify(group, null, 2));
