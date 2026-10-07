import fs from 'fs';
import path from 'path';
import { benchmarkSuite } from 'jest-bench';

import { loadNative } from '../js';

// Needs the bridge test harness: `yarn native:build-release-bridge-tests`.
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

function loop(kind: 'json' | 'sqlTemplates', value: unknown, iterations: number) {
  native.__testBridgeDeserializeLoop(kind, value, iterations);
}

describe('NativeSerdeDeserializer', () => {
  benchmarkSuite('sql templates', {
    'PostgresQuery sqlTemplates() (1 call)': () => {
      native.__testBridgeDeserializeSqlTemplates(sqlTemplates);
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
});
