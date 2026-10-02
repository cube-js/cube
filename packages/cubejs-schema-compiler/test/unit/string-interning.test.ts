import vm from 'vm';
import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { internString, internStringsDeep, internedStringsStats } from '../../src/compiler/StringInterning';
import { Compiler } from '../../src/compiler/PrepareCompiler';
import { prepareCompiler } from './PrepareCompiler';

const files = [
  {
    fileName: 'orders.js',
    content: `
const generated = (cubeName) => Object.fromEntries([1, 2, 3].map((i) => [
  'filtered_count_' + i,
  {
    type: 'count',
    title: 'Filtered count ' + i + ' of ' + cubeName,
    description: \`Rows with status \${i}\`,
    filters: [{ sql: new Function('CUBE', 'return \`\${CUBE}.status = ' + i + '\`') }],
  },
]));

cube('orders', {
  sql_table: 'public.orders',
  title: 'Orders',
  description: 'All ' + 'orders',
  joins: {
    users: { relationship: 'many_to_one', sql: \`\${CUBE}.user_id = \${users}.id\` },
  },
  measures: {
    count: { type: 'count', description: 'Number of orders' },
    amount: { sql: 'amount', type: 'sum', format: 'currency' },
    ...generated('orders'),
  },
  dimensions: {
    id: { sql: 'id', type: 'number', primaryKey: true },
    status: { sql: 'status', type: 'string', meta: { tags: ['a', 'b'], owner: 'team' + '-x' } },
    createdAt: { sql: 'created_at', type: 'time' },
  },
  segments: {
    completed: { sql: \`\${CUBE}.status = 'completed'\` },
  },
});
`,
  },
  {
    fileName: 'users.yml',
    content: `
cubes:
  - name: users
    sql_table: public.users
    description: Users of the shop
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: name
        sql: "{CUBE}.first_name || ' ' || {CUBE}.last_name"
        type: string
        title: Full name
    measures:
      - name: count
        type: count

views:
  - name: orders_view
    cubes:
      - join_path: orders
        includes: "*"
      - join_path: orders.users
        prefix: true
        includes:
          - name
`,
  },
];

const compileModel = async (options: { multiTenantSharing?: boolean, internStrings?: boolean }) => {
  const compilers = prepareCompiler(files.map((f) => ({ ...f })), options);
  await compilers.compiler.compile();
  return compilers;
};

// Builds a string at runtime (a ConsString), not a literal the parser already internalized
const cat = (...parts: string[]) => parts.join('');

const querySql = (compilers: Compiler, query: Record<string, unknown>) => new PostgresQuery(compilers, query).buildSqlAndParams();

describe('String interning', () => {
  test('internString keeps the value', () => {
    const built = cat('Filtered count ', String(Math.random()));
    expect(internString(built)).toBe(built);
    expect(internString('')).toBe('');
    expect(internString('42')).toBe('42');
    expect(internString('__proto__')).toBe('__proto__');
    expect(internString('constructor')).toBe('constructor');
  });

  test('internStringsDeep replaces data properties only', () => {
    let getterCalls = 0;
    class Holder {
      public value = cat('class', ' instance');
    }
    const frozen = Object.freeze({ title: cat('fro', 'zen') });
    const root: any = {
      title: cat('Ti', 'tle'),
      nested: { list: [cat('a', 'b'), 1, null, { deep: cat('de', 'ep') }] },
      fn: () => 'x',
      holder: new Holder(),
      frozen,
      get lazy() {
        getterCalls++;
        return { title: 'lazy' };
      },
    };
    Object.defineProperty(root, 'readOnly', { value: cat('read', 'only'), writable: false, enumerable: true });
    root.self = root;

    const before = internedStringsStats().calls;
    expect(internStringsDeep(root)).toBe(root);

    expect(getterCalls).toBe(0);
    expect(root.title).toBe('Title');
    expect(root.nested.list).toEqual(['ab', 1, null, { deep: 'deep' }]);
    expect(root.holder.value).toBe('class instance');
    expect(root.frozen).toBe(frozen);
    expect(root.readOnly).toBe('readonly');
    expect(root.self).toBe(root);
    // title, ab, deep, frozen.title, readOnly: the class instance is not walked
    expect(internedStringsStats().calls - before).toBe(5);
  });

  test('internStringsDeep walks objects created in another realm', () => {
    // Model files run in a vm context: their objects inherit from that context's Object
    const foreign = vm.runInNewContext('({ title: ["Fo", "reign"].join(""), list: [{ name: "a" + Math.random() }] })');
    class Local {
      public title = cat('lo', 'cal');
    }
    const foreignClass = vm.runInNewContext('new (class Foo { constructor() { this.title = "x" + Math.random(); } })()');
    const guarded = new Proxy({}, {
      ownKeys: () => {
        throw new Error('no access');
      },
    });

    const before = internedStringsStats().calls;
    internStringsDeep({ foreign, local: new Local(), foreignClass, guarded });
    // title and list[0].name: class instances of either realm are not walked, the proxy is skipped
    expect(internedStringsStats().calls - before).toBe(2);
    expect(foreign.title).toBe('Foreign');
  });

  test.each([
    ['interning only', { multiTenantSharing: false, internStrings: true }],
    ['multi-tenant sharing', { multiTenantSharing: true }],
  ])('compiled model is the same with and without interning (%s)', async (_name, options) => {
    const plain = await compileModel({ multiTenantSharing: false });
    const interned = await compileModel(options);

    expect(interned.metaTransformer.cubes).toEqual(plain.metaTransformer.cubes);
    expect(JSON.stringify(interned.metaTransformer.cubes)).toEqual(JSON.stringify(plain.metaTransformer.cubes));

    for (const name of plain.cubeEvaluator.cubeNames()) {
      const a = plain.cubeEvaluator.cubeFromPath(name);
      const b = interned.cubeEvaluator.cubeFromPath(name);
      // JSON drops the member functions, which are distinct closures in the two compiles
      expect(JSON.stringify(b.measures)).toEqual(JSON.stringify(a.measures));
      expect(JSON.stringify(b.dimensions)).toEqual(JSON.stringify(a.dimensions));
      expect(JSON.stringify(b.segments)).toEqual(JSON.stringify(a.segments));
    }

    const queries = [
      {
        measures: ['orders.count', 'orders.amount', 'orders.filtered_count_2', 'users.count'],
        dimensions: ['orders.status', 'users.name'],
        timeDimensions: [{ dimension: 'orders.createdAt', granularity: 'day', dateRange: ['2024-01-01', '2024-01-31'] }],
        segments: ['orders.completed'],
        timezone: 'UTC',
      },
      {
        measures: ['orders_view.count', 'orders_view.filtered_count_1'],
        dimensions: ['orders_view.users_name'],
        timezone: 'UTC',
      },
    ];

    for (const query of queries) {
      expect(querySql(interned, query)).toEqual(querySql(plain, query));
    }
  });

  test('interning walks the compiled model only when enabled', async () => {
    const before = internedStringsStats().calls;
    await compileModel({ multiTenantSharing: false });
    expect(internedStringsStats().calls).toBe(before);

    await compileModel({ multiTenantSharing: true });
    expect(internedStringsStats().calls).toBeGreaterThan(before);
  });

  test('defaults to CUBEJS_COMPILER_MULTI_TENANT_SHARING', async () => {
    const previous = process.env.CUBEJS_COMPILER_MULTI_TENANT_SHARING;

    try {
      process.env.CUBEJS_COMPILER_MULTI_TENANT_SHARING = 'true';
      const before = internedStringsStats().calls;
      await compileModel({});
      expect(internedStringsStats().calls).toBeGreaterThan(before);

      process.env.CUBEJS_COMPILER_MULTI_TENANT_SHARING = 'false';
      const after = internedStringsStats().calls;
      await compileModel({});
      expect(internedStringsStats().calls).toBe(after);
    } finally {
      if (previous === undefined) {
        delete process.env.CUBEJS_COMPILER_MULTI_TENANT_SHARING;
      } else {
        process.env.CUBEJS_COMPILER_MULTI_TENANT_SHARING = previous;
      }
    }
  });
});
