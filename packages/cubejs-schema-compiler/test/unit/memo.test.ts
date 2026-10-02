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
  ])('reports %s key', async (_name, key) => {
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
