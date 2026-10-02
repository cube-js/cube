import { prepareCompiler } from './PrepareCompiler';

type Files = { fileName: string, content: string }[];

const fetchFiles = (): Files => [{
  fileName: 'orders.js',
  content: `
    asyncModule(async () => {
      // Two files (or two calls in one file) asking for the same key share one invocation
      const [columns, sameColumns] = await Promise.all([
        memo('columns', () => COMPILE_CONTEXT.api.fetch('columns')),
        memo('columns', () => COMPILE_CONTEXT.api.fetch('columns')),
      ]);
      const table = await memo(['table', 'orders'], async () => COMPILE_CONTEXT.api.fetch('table'));

      cube('orders', {
        sql_table: table,

        measures: {
          count: { type: 'count' },
        },

        dimensions: Object.fromEntries(columns.concat(sameColumns).map((c) => [c, { sql: c, type: 'string' }])),
      });
    });
  `,
}, {
  fileName: 'order_items.js',
  content: `
    asyncModule(async () => {
      const columns = await memo('columns', () => COMPILE_CONTEXT.api.fetch('columns'));

      cube('order_items', {
        sql_table: 'order_items',

        dimensions: Object.fromEntries(columns.map((c) => [c, { sql: c, type: 'string' }])),
      });
    });
  `,
}];

const mockApi = () => {
  const calls: string[] = [];
  return {
    calls,
    api: {
      fetch: async (what: string) => {
        calls.push(what);
        await new Promise((resolve) => setTimeout(resolve, 1));
        return what === 'columns' ? [`id_${calls.length}`] : `orders_${calls.length}`;
      },
    },
  };
};

describe.each([
  ['off', false],
  ['on', true],
])('memo, shared VM context %s', (_name, sharedVmContext) => {
  it('invokes the function once per key across all compile stages', async () => {
    const { calls, api } = mockApi();
    const { compiler, cubeEvaluator } = prepareCompiler(fetchFiles(), {
      compileContext: { api },
      sharedVmContext,
    });
    await compiler.compile();

    expect(calls.sort()).toEqual(['columns', 'table']);
    const orders = cubeEvaluator.cubeFromPath('orders');
    const orderItems = cubeEvaluator.cubeFromPath('order_items');
    // Every stage saw the same results
    expect(Object.keys(orders.dimensions)).toEqual(Object.keys(orderItems.dimensions));
    expect(orders.sqlTable!()).toMatch(/^orders_\d$/);
  });

  it('does not share results between compiles', async () => {
    const { calls, api } = mockApi();
    await prepareCompiler(fetchFiles(), { compileContext: { api }, sharedVmContext }).compiler.compile();
    await prepareCompiler(fetchFiles(), { compileContext: { api }, sharedVmContext }).compiler.compile();

    expect(calls.sort()).toEqual(['columns', 'columns', 'table', 'table']);
  });

  it('works outside asyncModule', async () => {
    const calls: string[] = [];
    const { compiler, cubeEvaluator } = prepareCompiler([{
      fileName: 'orders.js',
      content: `
        const table = memo('table', () => COMPILE_CONTEXT.tableFor('orders'));

        cube('orders', {
          sql_table: table,
          measures: { count: { type: 'count' } },
        });
      `,
    }], {
      compileContext: {
        tableFor: (name: string) => {
          calls.push(name);
          return `${name}_table`;
        },
      },
      sharedVmContext,
    });
    await compiler.compile();

    expect(calls).toEqual(['orders']);
    expect(cubeEvaluator.cubeFromPath('orders').sqlTable!()).toEqual('orders_table');
  });

  it('caches a synchronous throw like an async rejection', async () => {
    const calls: string[] = [];
    const { compiler } = prepareCompiler([{
      fileName: 'orders.js',
      content: `
        let table;
        try {
          table = memo('table', () => COMPILE_CONTEXT.tableFor('orders'));
        } catch (e) {
          table = 'fallback';
        }

        cube('orders', {
          sql_table: table,
          measures: { count: { type: 'count' } },
        });
      `,
    }], {
      compileContext: {
        tableFor: (name: string) => {
          calls.push(name);
          throw new Error('API is down');
        },
      },
      sharedVmContext,
    });
    await compiler.compile();

    expect(calls).toEqual(['orders']);
  });

  it.each([
    ['cube', 'cube(\'orders\', { sql_table: \'orders\' })'],
    ['view', 'view(\'orders_view\', { cubes: [] })'],
    ['context', 'context(\'ctx\', { contextMembers: [] })'],
    ['view_group', 'view_group(\'group\', { views: [] })'],
    ['asyncModule', 'asyncModule(async () => {})'],
  ])('rejects %s() called from the memoized function', async (globalName, call) => {
    const sync = prepareCompiler([{
      fileName: 'orders.js',
      content: `memo('defs', () => { ${call}; });`,
    }], { sharedVmContext });
    await expect(sync.compiler.compile()).rejects.toThrow(`${globalName}() can't be called from the function passed to memo('defs')`);

    const async = prepareCompiler([{
      fileName: 'orders.js',
      content: `
        asyncModule(async () => {
          await memo('defs', async () => {
            await null;
            ${call};
          });
        });
      `,
    }], { sharedVmContext });
    await expect(async.compiler.compile()).rejects.toThrow(`${globalName}() can't be called from the function passed to memo('defs')`);
  });

  it('generates the key when it is omitted', async () => {
    const { calls, api } = mockApi();
    const { compiler, cubeEvaluator } = prepareCompiler([{
      fileName: 'orders.js',
      content: `
        asyncModule(async () => {
          const columns = await memo(() => COMPILE_CONTEXT.api.fetch('columns'));
          const table = await memo(async () => COMPILE_CONTEXT.api.fetch('table'));

          cube('orders', {
            sql_table: table,
            measures: { count: { type: 'count' } },
            dimensions: Object.fromEntries(columns.map((c) => [c, { sql: c, type: 'string' }])),
          });
        });
      `,
    }, {
      fileName: 'order_items.js',
      content: `
        asyncModule(async () => {
          // Same position as in orders.js, a different file: a different key
          const columns = await memo(() => COMPILE_CONTEXT.api.fetch('columns'));

          cube('order_items', {
            sql_table: 'order_items',
            dimensions: Object.fromEntries(columns.map((c) => [c, { sql: c, type: 'string' }])),
          });
        });
      `,
    }], { compileContext: { api }, sharedVmContext });
    await compiler.compile();

    // Once per call site for the whole compile
    expect(calls.sort()).toEqual(['columns', 'columns', 'table']);
    expect(cubeEvaluator.cubeFromPath('orders').sqlTable!()).toMatch(/^orders_\d$/);
  });

  it.each([
    ['a function', `
      const tableFor = (name) => memo(() => COMPILE_CONTEXT.tableFor(name));
      const tables = [tableFor('orders'), tableFor('users')];
    `],
    ['a loop', `
      const tables = [];
      for (const name of ['orders', 'users']) {
        tables.push(memo(() => COMPILE_CONTEXT.tableFor(name)));
      }
    `],
  ])('rejects a keyless memo() in %s', async (_place, content) => {
    const { compiler } = prepareCompiler([{ fileName: 'orders.js', content }], {
      compileContext: { tableFor: (name: string) => name },
      sharedVmContext,
    });

    await expect(compiler.compile()).rejects.toThrow(/memo\(\) at orders\.js:\d+:\d+ is called more than once per compile stage/);
  });

  it('allows a keyless memo() in a function called once per stage', async () => {
    const calls: string[] = [];
    const { compiler, cubeEvaluator } = prepareCompiler([{
      fileName: 'orders.js',
      content: `
        const tableFor = (name) => memo(() => COMPILE_CONTEXT.tableFor(name));
        // sql_table itself is evaluated lazily, after the compile
        const table = tableFor('orders');
        cube('orders', { sql_table: table, measures: { count: { type: 'count' } } });
      `,
    }], {
      compileContext: {
        tableFor: (name: string) => {
          calls.push(name);
          return `${name}_table`;
        },
      },
      sharedVmContext,
    });
    await compiler.compile();

    expect(calls).toEqual(['orders']);
    expect(cubeEvaluator.cubeFromPath('orders').sqlTable!()).toEqual('orders_table');
  });

  it('calls the function each time from members evaluated after the compile', async () => {
    const calls: string[] = [];
    const { compiler, cubeEvaluator } = prepareCompiler([{
      fileName: 'orders.js',
      content: `
        cube('orders', {
          sql_table: memo(() => COMPILE_CONTEXT.tableFor('orders')),
          measures: { count: { type: 'count' } },
        });
      `,
    }], {
      compileContext: {
        tableFor: (name: string) => {
          calls.push(name);
          return `${name}_table`;
        },
      },
      sharedVmContext,
    });
    await compiler.compile();
    const callsAfterCompile = calls.length;

    const orders = cubeEvaluator.cubeFromPath('orders');
    expect(orders.sqlTable!()).toEqual('orders_table');
    expect(orders.sqlTable!()).toEqual('orders_table');
    expect(calls.length).toEqual(callsAfterCompile + 2);
  });

  it('calls the function from members that compilers evaluate', async () => {
    const calls: string[] = [];
    const joined = (name: string) => `
      cube('${name}', {
        sql_table: '${name}',
        joins: { users: { relationship: 'many_to_one', sql: \`\${CUBE}.user_id = \${users}.id\` } },
        measures: { count: { type: 'count' } },
        dimensions: { id: { sql: 'id', type: 'number', primary_key: true } },
      });
    `;
    const { compiler } = prepareCompiler([{
      fileName: 'users.js',
      content: `
        cube('users', {
          sql_table: 'users',
          // The join graph evaluates it once per joining cube
          measures: { total: { sql: memo(() => COMPILE_CONTEXT.columnFor('total')), type: 'sum' } },
          dimensions: { id: { sql: 'id', type: 'number', primary_key: true } },
        });
      `,
    }, {
      fileName: 'orders.js',
      content: joined('orders'),
    }, {
      fileName: 'items.js',
      content: joined('items'),
    }], {
      compileContext: {
        columnFor: (name: string) => {
          calls.push(name);
          return name;
        },
      },
      sharedVmContext,
    });

    await compiler.compile();
    expect(calls.length).toBeGreaterThanOrEqual(2);
  });

  it('reports invalid arguments', async () => {
    const { compiler } = prepareCompiler([{
      fileName: 'orders.js',
      content: `
        memo('table', 'orders');
      `,
    }], { sharedVmContext });

    await expect(compiler.compile()).rejects.toThrow('memo() expects a function as its second argument');
  });

  it('keeps string keys apart from other keys', async () => {
    const { compiler, cubeEvaluator } = prepareCompiler([{
      fileName: 'orders.js',
      content: `
        const table = [memo(1, () => 'a'), memo('1', () => 'b'), memo(['x'], () => 'c'), memo('["x"]', () => 'd')].join('_');

        cube('orders', {
          sql_table: table,
          measures: { count: { type: 'count' } },
        });
      `,
    }], { sharedVmContext });
    await compiler.compile();

    expect(cubeEvaluator.cubeFromPath('orders').sqlTable!()).toEqual('a_b_c_d');
  });

  it.each([
    ['undefined', 'undefined'],
    ['a BigInt', '10n'],
    ['a cyclic structure', '(() => { const o = {}; o.o = o; return o; })()'],
  ])('reports %s key', async (_kind, key) => {
    const { compiler } = prepareCompiler([{
      fileName: 'orders.js',
      content: `memo(${key}, () => 1);`,
    }], { sharedVmContext });

    await expect(compiler.compile()).rejects.toThrow('memo() expects a string or a JSON-serializable key as its first argument');
  });

  it('reports memo(fn) that the transpiler did not give a key', async () => {
    const { compiler } = prepareCompiler([{
      fileName: 'orders.js',
      content: `
        const fn = () => 'orders';
        const args = [fn];
        memo(...args);
      `,
    }], { sharedVmContext });

    await expect(compiler.compile()).rejects.toThrow('memo() expects a key as its first argument: memo(key, fn)');
  });
});
