import fs from 'fs';
import path from 'path';
import { benchmarkSuite } from 'jest-bench';

import { loadNative } from '../js';

// Needs a release build with the bridge test harness (python-config.bench.ts also needs `python`):
// `yarn native:build -- --release --features python,bridge-test-harness`.
// A debug build makes the numbers meaningless.
const native = loadNative();

const sqlTemplates = JSON.parse(
  fs.readFileSync(path.join(process.cwd(), 'benchmarks', 'fixtures', 'sql-templates.json'), 'utf8')
);

const wideStrings: Record<string, string> = {};

for (let i = 0; i < 1000; i += 1) {
  wideStrings[`member_${i}`] = `"orders".amount_${i} * 100 / NULLIF("orders".count, 0)`;
}

const numbers = Array.from({ length: 10_000 }, (_, i) => (i % 3 === 0 ? i + 0.5 : i));

const nestedObjects = Array.from({ length: 200 }, (_, i) => ({
  name: `cube_${i}.member_${i}`,
  type: i % 2 === 0 ? 'number' : 'string',
  public: i % 5 !== 0,
  title: null,
  meta: { owner: 'analytics', priority: i, tags: ['a', 'b', 'c'] },
  granularities: [{ name: 'fiscal_year', interval: '1 year', offset: '3 months' }],
}));

// Shape of a compiled cube (`CubeEvaluator.cubeFromPath`): 13 own properties, of which
// CubeDefinitionStatic declares only `name`.
const measure = (i: number) => ({
  type: 'sum',
  sql: () => `amount_${i}`,
  title: `Total ${i}`,
  format: 'currency',
  meta: { a: i },
  ownedByCube: true,
});

const cube = {
  allDefinitions: () => ({}),
  rawFolders: () => [],
  rawCubes: () => [],
  preAggregations: { main: { type: 'rollup' } },
  joins: [{ name: 'users', relationship: 'many_to_one', sql: () => '' }],
  measures: Object.fromEntries(Array.from({ length: 30 }, (_, i) => [`m${i}`, measure(i)])),
  dimensions: Object.fromEntries(
    Array.from({ length: 30 }, (_, i) => [`d${i}`, { sql: () => `d${i}`, type: 'string', ownedByCube: true }])
  ),
  segments: {},
  hierarchies: {},
  accessPolicy: undefined,
  sql: () => 'select * from orders',
  name: 'orders',
  fileName: 'orders.js',
};

type LoopKind = 'json' | 'sqlTemplates' | 'cubeStatic' | 'measureStatic';

function loop(kind: LoopKind, value: unknown, iterations: number) {
  native.__testBridgeDeserializeLoop(kind, value, iterations);
}

describe('NativeSerdeDeserializer', () => {
  benchmarkSuite('sql templates', {
    'PostgresQuery sqlTemplates() (1 call)': () => {
      loop('sqlTemplates', sqlTemplates, 1);
    },
    'PostgresQuery sqlTemplates() x100 in Rust': () => {
      loop('sqlTemplates', sqlTemplates, 100);
    },
  });

  benchmarkSuite('generic shapes', {
    'wide object, 1000 string fields': () => {
      loop('json', wideStrings, 1);
    },
    'array of 10k numbers': () => {
      loop('json', numbers, 1);
    },
    '200 nested member-like objects': () => {
      loop('json', nestedObjects, 1);
    },
  });

  benchmarkSuite('static bridge structs', {
    'CubeDefinitionStatic from a compiled cube (x100 in Rust)': () => {
      loop('cubeStatic', cube, 100);
    },
    'MeasureDefinitionStatic from a measure (x100 in Rust)': () => {
      loop('measureStatic', measure(1), 100);
    },
  });
});
