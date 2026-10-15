/* eslint-disable no-console, no-template-curly-in-string, @stylistic/padding-line-between-statements */
// Heap retained per compiled tenant for a multi-tenant deployment: every tenant has the SAME
// model files (byte-identical) and differs only by COMPILE_CONTEXT.securityContext.seed, from
// which calculated_fields.js generates per-tenant measures and dimensions. One CompilerApi per
// tenant, like the server's compilerCache holds one per app id. ONE configuration per process:
//
//   yarn tsc   # from the repo root, the script loads dist/
//   node --expose-gc benchmarks/multi-tenant-compile-memory.js \
//     --sharing=false --tenants=12 --cubes=1250 [--snapshot=out.heapsnapshot]
//
// --sharing sets CUBEJS_COMPILER_MULTI_TENANT_SHARING (shared VM realm, process-wide compile
// caches and string interning).
//
// --queries=N then times N SQL generations (CompilerApi.getSql) on the first tenant.
//
// All compiled CompilerApis stay referenced until the measurement is taken. Per-tenant numbers
// include fixed costs spread over the tenants: run --tenants=6 and 12 and difference the totals
// for the marginal cost of one more tenant. Summarize a snapshot with
//   node --max-old-space-size=12000 ../cubejs-schema-compiler/benchmarks/heap-snapshot-summary.js out.heapsnapshot --strings=20

const v8 = require('v8');
const path = require('path');

const args = Object.fromEntries(process.argv.slice(2).map((a) => {
  const [k, v] = a.replace(/^--/, '').split('=');
  return [k, v === undefined ? 'true' : v];
}));

const sharing = args.sharing === 'true';
const tenants = parseInt(args.tenants || '12', 10);
const cubes = parseInt(args.cubes || '1250', 10);
// Base members per cube: id + dimensions, plus measures.
const dimensionsPerCube = parseInt(args.dimensions || '9', 10);
const measuresPerCube = parseInt(args.measures || '3', 10);
// Fraction of cubes authored in JS (the rest are YAML). Only JS cubes get calculated members.
const jsRatio = parseFloat(args.jsRatio || '0.5');
// Calculated members generated per JS cube: min..max, picked per tenant seed.
const calcMin = parseInt(args.calcMin || '2', 10);
const calcMax = parseInt(args.calcMax || '12', 10);

process.env.CUBEJS_COMPILER_MULTI_TENANT_SHARING = String(sharing);

// eslint-disable-next-line import/no-dynamic-require
const { CompilerApi } = require(path.resolve(__dirname, '../dist/src/core/CompilerApi'));

if (typeof global.gc !== 'function') {
  throw new Error('Run with node --expose-gc');
}

const joinTarget = (i) => (i > 0 ? Math.floor((i - 1) / 2) : null);
const words = ['customer', 'order', 'product', 'region', 'channel', 'status', 'category', 'segment', 'account', 'campaign'];
const word = (i, j) => words[(i * 7 + j * 3) % words.length];

const yamlCube = (i) => {
  const lines = [
    'cubes:',
    `  - name: c${i}`,
    `    sql_table: analytics.table_${i}`,
    `    title: ${word(i, 0)} facts ${i}`,
    `    description: Facts about ${word(i, 0)} and ${word(i, 1)} for table ${i}`,
  ];
  const target = joinTarget(i);
  if (target !== null) {
    lines.push(
      '    joins:',
      `      - name: c${target}`,
      '        relationship: many_to_one',
      `        sql: "{CUBE}.fk_${target} = {c${target}}.id"`,
    );
  }
  lines.push('    dimensions:', '      - name: id', '        sql: id', '        type: number', '        primary_key: true');
  for (let j = 1; j <= dimensionsPerCube; j++) {
    lines.push(
      `      - name: ${word(i, j)}_${j}`,
      `        sql: "{CUBE}.col_${j}"`,
      j % 4 === 0 ? '        type: time' : '        type: string',
      `        description: The ${word(i, j)} attribute number ${j}`,
    );
  }
  lines.push('    measures:', '      - name: count', '        type: count');
  for (let j = 1; j < measuresPerCube; j++) {
    lines.push(`      - name: amount_${j}`, `        sql: "{CUBE}.amount_${j}"`, '        type: sum', `        title: Total amount ${j}`);
  }
  return lines.join('\n');
};

const jsCube = (i) => {
  const dims = ['    id: { sql: `id`, type: `number`, primaryKey: true },'];
  for (let j = 1; j <= dimensionsPerCube; j++) {
    dims.push(`    ${word(i, j)}_${j}: { sql: \`\${CUBE}.col_${j}\`, type: \`${j % 4 === 0 ? 'time' : 'string'}\`, description: \`The ${word(i, j)} attribute number ${j}\` },`);
  }
  const meas = ['    count: { type: `count` },'];
  for (let j = 1; j < measuresPerCube; j++) {
    meas.push(`    amount_${j}: { sql: \`\${CUBE}.amount_${j}\`, type: \`sum\`, title: \`Total amount ${j}\` },`);
  }
  const target = joinTarget(i);
  const joins = target === null ? '' : `  joins: {
    c${target}: { relationship: \`many_to_one\`, sql: \`\${CUBE}.fk_${target} = \${c${target}}.id\` },
  },
`;
  return `import { calculatedMeasures, calculatedDimensions } from './calculated_fields';

cube(\`c${i}\`, {
  sql_table: \`analytics.table_${i}\`,
  title: \`${word(i, 0)} facts ${i}\`,
  description: \`Facts about ${word(i, 0)} and ${word(i, 1)} for table ${i}\`,
${joins}  dimensions: {
${dims.join('\n')}
    ...calculatedDimensions('c${i}'),
  },
  measures: {
${meas.join('\n')}
    ...calculatedMeasures('c${i}'),
  },
});
`;
};

// Reads the tenant seed from COMPILE_CONTEXT and deterministically generates members from it.
// Member sql is built with `new Function(...args, body)` so the parameter names are references.
const calculatedFields = () => `const seed = String(COMPILE_CONTEXT.securityContext.seed);

const hash = (s) => {
  let h = 2166136261;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return h >>> 0;
};

const pick = (cubeName, salt, n) => hash(seed + ':' + cubeName + ':' + salt) % n;

export function calculatedMeasures(cubeName) {
  const count = ${calcMin} + pick(cubeName, 'measures', ${calcMax - calcMin + 1});
  const result = {};
  for (let j = 0; j < count; j++) {
    const kind = pick(cubeName, 'kind' + j, 3);
    const col = 1 + pick(cubeName, 'col' + j, ${dimensionsPerCube});
    const value = pick(cubeName, 'value' + j, 50);
    if (kind === 0) {
      result['filtered_count_' + j] = {
        type: 'count',
        title: 'Filtered count ' + j + ' (tenant ' + seed + ')',
        description: 'Number of rows where col_' + col + ' equals v' + value + ' for tenant ' + seed,
        filters: [{ sql: new Function('CUBE', 'return \`\${CUBE}.col_' + col + " = 'v" + value + "'\`") }],
      };
    } else if (kind === 1) {
      result['ratio_' + j] = {
        type: 'number',
        format: 'percent',
        title: 'Ratio ' + j + ' (tenant ' + seed + ')',
        description: 'Amount 1 over count, calculated for tenant ' + seed,
        sql: new Function('amount_1', 'count', 'return \`\${amount_1} / NULLIF(\${count}, 0)\`'),
      };
    } else {
      result['filtered_amount_' + j] = {
        type: 'sum',
        title: 'Filtered amount ' + j + ' (tenant ' + seed + ')',
        description: 'Sum of amount_1 where col_' + col + ' > ' + value + ' for tenant ' + seed,
        sql: new Function('CUBE', 'return \`\${CUBE}.amount_1\`'),
        filters: [{ sql: new Function('CUBE', 'return \`\${CUBE}.col_' + col + ' > ' + value + '\`') }],
      };
    }
  }
  return result;
}

export function calculatedDimensions(cubeName) {
  const count = 1 + pick(cubeName, 'dimensions', ${Math.max(1, Math.round((calcMax - calcMin) / 3))});
  const result = {};
  for (let j = 0; j < count; j++) {
    const threshold = pick(cubeName, 'threshold' + j, 1000);
    result['bucket_' + j] = {
      type: 'string',
      title: 'Bucket ' + j + ' (tenant ' + seed + ')',
      description: 'Bucket of amount over ' + threshold + ' for tenant ' + seed,
      sql: new Function('CUBE', 'return \`CASE WHEN \${CUBE}.amount_1 > ' + threshold + " THEN 'high' ELSE 'low' END\`"),
    };
  }
  return result;
}
`;

const modelFiles = (cubeCount) => {
  const files = [{ fileName: 'calculated_fields.js', content: calculatedFields() }];
  const jsEvery = jsRatio > 0 ? Math.round(1 / jsRatio) : 0;
  for (let i = 0; i < cubeCount; i++) {
    const isJs = jsEvery > 0 && i % jsEvery === 0;
    files.push(isJs
      ? { fileName: `c${i}.js`, content: jsCube(i) }
      : { fileName: `c${i}.yml`, content: yamlCube(i) });
  }
  return files;
};

// Byte-identical for every tenant, but each repository hands out its own copy of the strings
// (as a FileRepository reading from disk, or a per-tenant repositoryFactory, would).
const templateFiles = modelFiles(cubes);
const copyString = (s) => Buffer.from(s, 'utf8').toString('utf8');

const createTenant = (seed) => {
  const repository = {
    localPath: () => __dirname,
    dataSchemaFiles: () => Promise.resolve(templateFiles.map((f) => ({
      fileName: copyString(f.fileName),
      content: copyString(f.content),
    }))),
  };
  return new CompilerApi(repository, () => 'postgres', {
    compileContext: { securityContext: { seed } },
    compilerCacheSize: 250,
    allowNodeRequire: false,
    logger: () => undefined,
  });
};

// Several full GCs, so old bytecode gets flushed as it eventually would in a long-running server.
const gcRounds = parseInt(args.gcRounds || '6', 10);
const settle = async () => {
  for (let i = 0; i < gcRounds; i++) {
    global.gc();
    await new Promise((resolve) => setImmediate(resolve));
  }
};

const mb = (bytes) => bytes / 1024 / 1024;

const sample = () => {
  const mem = process.memoryUsage();
  return { heapUsed: mem.heapUsed, rss: mem.rss, external: mem.external };
};

(async () => {
  // Warm up module-level state with a small model, then drop it.
  {
    const warm = new CompilerApi({
      localPath: () => __dirname,
      dataSchemaFiles: () => Promise.resolve(modelFiles(10)),
    }, () => 'postgres', { compileContext: { securityContext: { seed: 'warmup' } }, allowNodeRequire: false, logger: () => undefined });
    await warm.getCompilers();
    warm.dispose();
  }
  await settle();
  const baseline = sample();

  const retained = [];
  const compileMs = [];
  for (let t = 0; t < tenants; t++) {
    const api = createTenant(`seed${1000 + t}`);
    const started = process.hrtime.bigint();
    await api.getCompilers();
    compileMs.push(Number(process.hrtime.bigint() - started) / 1e6);
    retained.push(api);
  }
  await settle();
  const after = sample();

  const countMembers = async (api) => {
    const { cubeEvaluator } = await api.getCompilers();
    return cubeEvaluator.cubeNames()
      .map((name) => {
        const cube = cubeEvaluator.cubeFromPath(name);
        return Object.keys(cube.measures || {}).length + Object.keys(cube.dimensions || {}).length;
      })
      .reduce((a, b) => a + b, 0);
  };

  const sorted = [...compileMs].sort((a, b) => a - b);
  const result = {
    sharing,
    tenants,
    cubes,
    membersPerTenant: [await countMembers(retained[0]), await countMembers(retained[tenants - 1])],
    compileMsPerTenantMean: +(compileMs.reduce((a, b) => a + b, 0) / tenants).toFixed(0),
    compileMsFirst: +compileMs[0].toFixed(0),
    compileMsMedian: +sorted[Math.floor(tenants / 2)].toFixed(0),
    heapUsedMbPerTenant: +(mb(after.heapUsed - baseline.heapUsed) / tenants).toFixed(2),
    rssMbPerTenant: +(mb(after.rss - baseline.rss) / tenants).toFixed(2),
    externalMbPerTenant: +(mb(after.external - baseline.external) / tenants).toFixed(2),
    heapUsedMbTotal: +mb(after.heapUsed).toFixed(1),
    rssMbTotal: +mb(after.rss).toFixed(1),
  };

  if (sharing) {
    // eslint-disable-next-line global-require
    const { internedStringsStats } = require('@cubejs-backend/schema-compiler');
    result.internPool = internedStringsStats();
  }

  // SQL generation through the compiled model (member functions run at planning time): the cost
  // of the shared realm's `with` scope shows up here, not in the compile.
  const queries = parseInt(args.queries || '0', 10);
  if (queries > 0) {
    const api = retained[0];
    const { cubeEvaluator } = await api.getCompilers();
    const names = cubeEvaluator.cubeNames().filter((n) => n !== 'c0');
    const pickQuery = (q) => {
      const name = names[(q * 7919) % names.length];
      const cube = cubeEvaluator.cubeFromPath(name);
      const target = `c${joinTarget(parseInt(name.slice(1), 10))}`;
      const targetCube = cubeEvaluator.cubeFromPath(target);
      const measures = Object.keys(cube.measures).slice(0, 4).map((m) => `${name}.${m}`);
      const calculated = Object.keys(cube.measures).filter((m) => !m.startsWith('amount') && m !== 'count').slice(0, 2);
      const dims = Object.entries(cube.dimensions).filter(([, d]) => d.type === 'string').slice(0, 2).map(([d]) => `${name}.${d}`);
      const timeDim = Object.entries(cube.dimensions).find(([, d]) => d.type === 'time');
      const targetDim = Object.entries(targetCube.dimensions).find(([, d]) => d.type === 'string');
      return {
        measures: [...measures, ...calculated.map((m) => `${name}.${m}`)],
        dimensions: [...dims, ...(targetDim ? [`${target}.${targetDim[0]}`] : [])],
        timeDimensions: timeDim ? [{ dimension: `${name}.${timeDim[0]}`, granularity: 'day', dateRange: ['2024-01-01', '2024-03-31'] }] : [],
        order: measures.length ? [[measures[0], 'desc']] : [],
        limit: 1000,
        timezone: 'UTC',
      };
    };
    for (let q = 0; q < 50; q++) {
      await api.getSql(pickQuery(q));
    }
    const times = [];
    for (let q = 0; q < queries; q++) {
      const query = pickQuery(q + 50);
      const started = process.hrtime.bigint();
      await api.getSql(query);
      times.push(Number(process.hrtime.bigint() - started) / 1e6);
    }
    times.sort((a, b) => a - b);
    result.sqlQueries = queries;
    result.sqlMsMedian = +times[Math.floor(queries / 2)].toFixed(2);
    result.sqlMsP95 = +times[Math.floor(queries * 0.95)].toFixed(2);
    result.sqlMsMean = +(times.reduce((a, b) => a + b, 0) / queries).toFixed(2);
  }

  if (args.snapshot) {
    result.snapshot = v8.writeHeapSnapshot(path.resolve(args.snapshot));
  }

  console.log(JSON.stringify(result));
  // Keep `retained` alive until after the measurement and the snapshot.
  if (retained.length !== tenants) {
    throw new Error('unreachable');
  }
  process.exit(0);
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
