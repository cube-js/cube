import { CubejsServerCore } from '../../src';

const schema = `cube('Orders', {
  sql: 'select 1 as id',
  measures: { count: { type: 'count' } },
  dimensions: { id: { sql: 'id', type: 'number', primaryKey: true } },
});`;

describe('getCompilerApi concurrency (#12780)', () => {
  test('concurrent first requests compile the schema once', async () => {
    const messages: string[] = [];
    const core = new CubejsServerCore(<any>{
      apiSecret: 'secret',
      driverFactory: () => <any>({ type: 'postgres' }),
      contextToAppId: () => 'CUBE_APP_a',
      schemaVersion: () => 'v1',
      preAggregationsSchema: async () => 'pre_aggregations_a',
      repositoryFactory: () => ({
        dataSchemaFiles: async () => [{ fileName: 'orders.js', content: schema }],
      }),
      logger: (msg: string) => messages.push(msg),
    });

    const ctx = (i: number) => <any>{ requestId: `r${i}`, securityContext: { tenant: 'a' } };
    const apis = await Promise.all(Array.from({ length: 10 }, (_, i) => core.getCompilerApi(ctx(i))));
    await Promise.all(apis.map((api, i) => api.metaConfig(ctx(i), { requestId: `r${i}` })));

    const compiles = messages.filter(m => m === 'Compiling schema' || m === 'Recompiling schema').length;
    console.log({ distinctApis: new Set(apis).size, compiles });
    await core.releaseConnections();

    expect(new Set(apis).size).toEqual(1);
    expect(compiles).toEqual(1);
  });
});
