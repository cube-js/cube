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

  it('reports invalid arguments', async () => {
    const { compiler } = prepareCompiler([{
      fileName: 'orders.js',
      content: `
        memo('table', 'orders');
      `,
    }], { sharedVmContext });

    await expect(compiler.compile()).rejects.toThrow('memo() expects a function as its second argument');
  });
});
