import { SchemaFileRepository } from '@cubejs-backend/shared';
import { BaseQuery, MysqlQuery, PostgresQuery } from '@cubejs-backend/schema-compiler';
import type { Compilers, QueryFactory } from '@cubejs-backend/schema-compiler';
import { CompilerApi } from '../../src/core/CompilerApi';
import { DbTypeInternalFn } from '../../src/core/types';

// Test helper class to expose protected properties
class CompilerApiTestable extends CompilerApi {
  public getCompilersProperty(): Promise<Compilers> | any {
    return this.compilers;
  }

  public getQueryFactoryProperty(): QueryFactory | any {
    return this.queryFactory;
  }
}

describe('CompilerApi', () => {
  describe('dispose', () => {
    let compilerApi: CompilerApiTestable;

    // Mock repository
    const mockRepository: SchemaFileRepository = {
      localPath: () => '/mock/path',
      dataSchemaFiles: () => Promise.resolve([
        {
          fileName: 'test.js',
          content: `
            cube('TestCube', {
              sql: 'SELECT * FROM test',
              measures: {
                count: {
                  type: 'count'
                }
              }
            });
          `
        }
      ])
    };

    // Mock dbType function
    const mockDbType: DbTypeInternalFn = async () => 'postgres';

    beforeEach(() => {
      compilerApi = new CompilerApiTestable(
        mockRepository,
        mockDbType,
        {
          logger: () => {}, // eslint-disable-line @typescript-eslint/no-empty-function
        }
      );
    });

    afterEach(() => {
      if (compilerApi) {
        compilerApi.dispose();
      }
    });

    test('should replace compilers with disposed proxy after dispose', async () => {
      await compilerApi.getCompilers();

      compilerApi.dispose();

      // Try to access compilers after dispose - should throw
      const compilers = compilerApi.getCompilersProperty();

      // Since compilers is now a disposed proxy (not a Promise),
      // any property access should throw immediately
      expect(() => compilers.cubeEvaluator).toThrow(/disposed CompilerApi instance/);
    });

    test('should replace queryFactory with disposed proxy after dispose', async () => {
      await compilerApi.getCompilers();

      compilerApi.dispose();

      // Try to access queryFactory - should throw
      const queryFactory = compilerApi.getQueryFactoryProperty();

      expect(() => queryFactory.createQuery).toThrow(/disposed CompilerApi instance/);
    });

    test('should set graphqlSchema to undefined on dispose', async () => {
      const mockSchema = {} as any;
      compilerApi.setGraphQLSchema(mockSchema);

      expect(compilerApi.getGraphQLSchema()).toBe(mockSchema);

      compilerApi.dispose();

      // Schema should be undefined
      expect(compilerApi.getGraphQLSchema()).toBeUndefined();
    });

    test('should be safe to call dispose multiple times', async () => {
      await compilerApi.getCompilers();

      compilerApi.dispose();
      compilerApi.dispose();
      compilerApi.dispose();

      // Should still throw on access
      const compilers = compilerApi.getCompilersProperty();

      expect(() => compilers.cubeEvaluator).toThrow(/disposed CompilerApi instance/);
    });
  });

  describe('getSql cache', () => {
    const model = (values: string) => `
cubes:
  - name: orders
    sql: SELECT * FROM orders
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: created_at
        sql: created_at
        type: time
    measures:
      - name: amount
        sql: amount
        type: sum
      - name: amount_in_range
        multi_stage: true
        type: number
        sql: "{amount}"
        filter:
          include:
            - member: orders.created_at
              operator: inDateRange
              values: ${values}
  - name: customers
    sql: SELECT * FROM customers
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
    measures:
      - name: count
        type: count
  - name: shops
    sql: SELECT * FROM shops
    joins:
      - name: orders
        alias: latest_order
        sql: "{CUBE}.latest_order_id = {latest_order}.id"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

views:
  - name: orders_view
    cubes:
      - join_path: orders
        includes:
          - amount_in_range
`;

    const compilerApiFor = (values: string) => new CompilerApi(
      {
        localPath: () => '/mock/path',
        dataSchemaFiles: () => Promise.resolve([{ fileName: 'orders.yml', content: model(values) }]),
      },
      async () => 'postgres',
      {
        logger: () => {}, // eslint-disable-line @typescript-eslint/no-empty-function
        sqlCache: true,
      }
    );

    const sqlInTwoMinutes = async (compilerApi: CompilerApi, measure = 'orders.amount_in_range') => {
      const query: any = { measures: [measure], timezone: 'UTC' };
      const now = jest.spyOn(Date, 'now').mockReturnValue(Date.UTC(2026, 9, 7, 12, 0, 10));

      try {
        const first = await compilerApi.getSql(query);
        const sameMinute = await compilerApi.getSql(query);
        now.mockReturnValue(Date.UTC(2026, 9, 7, 12, 1, 10));
        const nextMinute = await compilerApi.getSql(query);
        return { first, sameMinute, nextMinute };
      } finally {
        now.mockRestore();
        compilerApi.dispose();
      }
    };

    test('a relative date range in the data model is compiled again in the next minute', async () => {
      const { first, sameMinute, nextMinute } = await sqlInTwoMinutes(compilerApiFor('[today]'));
      expect(sameMinute).toBe(first);
      expect(nextMinute).not.toBe(first);
    });

    test('a view over a relative date range is compiled again in the next minute', async () => {
      const { first, nextMinute } = await sqlInTwoMinutes(compilerApiFor('[today]'), 'orders_view.amount_in_range');
      expect(nextMinute).not.toBe(first);
    });

    test('a cube instance over a relative date range is compiled again in the next minute', async () => {
      const { first, nextMinute } = await sqlInTwoMinutes(compilerApiFor('[today]'), 'shops.latest_order.amount_in_range');
      expect(nextMinute).not.toBe(first);
    });

    test('a cube without relative date ranges keeps the cached SQL', async () => {
      const { first, nextMinute } = await sqlInTwoMinutes(compilerApiFor('[today]'), 'customers.count');
      expect(nextMinute).toBe(first);
    });

    test('absolute date ranges keep the cached SQL', async () => {
      const { first, nextMinute } = await sqlInTwoMinutes(compilerApiFor('["2026-10-01", "2026-10-07"]'));
      expect(nextMinute).toBe(first);
    });
  });

  describe('model context', () => {
    const repository: SchemaFileRepository = {
      localPath: () => '/mock/path',
      dataSchemaFiles: () => Promise.resolve([
        {
          fileName: 'cubes.js',
          content: `
            cube('pg_orders', {
              sql: 'SELECT * FROM orders',
              measures: { count: { type: 'count' } },
              dimensions: { status: { sql: 'status', type: 'string' } },
            });
            cube('bq_orders', {
              sql: 'SELECT * FROM orders',
              dataSource: 'bigquery',
              measures: { count: { type: 'count' } },
              dimensions: { status: { sql: 'status', type: 'string' } },
            });
          `
        }
      ])
    };

    let compilerApi: CompilerApi;
    let sqlTemplates: jest.SpyInstance;

    beforeEach(() => {
      compilerApi = new CompilerApi(
        repository,
        // Unlike MysqlQuery, BigqueryQuery doesn't read its data source when constructed
        async ({ dataSource }) => (dataSource === 'bigquery' ? 'bigquery' : 'postgres'),
        {
          logger: () => {}, // eslint-disable-line @typescript-eslint/no-empty-function
          sqlCache: false,
        }
      );
      sqlTemplates = jest.spyOn(BaseQuery.prototype, 'sqlTemplates');
    });

    afterEach(() => {
      sqlTemplates.mockRestore();
      compilerApi.dispose();
    });

    const sql = async (cube: string) => (await compilerApi.getSql(
      { measures: [`${cube}.count`], dimensions: [`${cube}.status`], timezone: 'UTC' } as any
    )).sql[0];

    test('renders each data source in its own dialect', async () => {
      const pg = await sql('pg_orders');
      const bq = await sql('bq_orders');

      expect(pg).toContain('"pg_orders".status');
      expect(bq).toContain('`bq_orders`.status');
      expect(await sql('pg_orders')).toEqual(pg);
      expect(await sql('bq_orders')).toEqual(bq);
    });

    test('compiles the templates of a data source once per model', async () => {
      await sql('pg_orders');
      await sql('bq_orders');
      const calls = sqlTemplates.mock.calls.length;
      expect(calls).toBeGreaterThan(0);

      await sql('pg_orders');
      await sql('bq_orders');

      expect(sqlTemplates).toHaveBeenCalledTimes(calls);
    });

    test('renders a data source in its own dialect class when the dbType is the same', async () => {
      class BacktickPostgresQuery extends PostgresQuery {
        public sqlTemplates() {
          const templates = super.sqlTemplates();
          templates.quotes.identifiers = '`';
          templates.quotes.escape = '\\`';
          return templates;
        }
      }

      const api = new CompilerApi(
        {
          localPath: () => '/mock/path',
          dataSchemaFiles: () => Promise.resolve([{
            fileName: 'cubes.js',
            content: `
              cube('pg_orders', {
                sql: 'SELECT * FROM orders',
                measures: { count: { type: 'count' } },
                dimensions: { status: { sql: 'status', type: 'string' } },
              });
              cube('analytics_orders', {
                sql: 'SELECT * FROM orders',
                dataSource: 'analytics',
                measures: { count: { type: 'count' } },
                dimensions: { status: { sql: 'status', type: 'string' } },
              });
            `
          }]),
        },
        async () => 'postgres',
        {
          logger: () => {}, // eslint-disable-line @typescript-eslint/no-empty-function
          sqlCache: false,
          dialectClass: ({ dataSource }) => (dataSource === 'analytics' ? BacktickPostgresQuery : undefined) as typeof BaseQuery,
        }
      );
      const apiSql = async (cube: string) => (await api.getSql(
        { measures: [`${cube}.count`], dimensions: [`${cube}.status`], timezone: 'UTC' } as any
      )).sql[0];

      try {
        // Either one first must not leave its templates to the other
        expect(await apiSql('analytics_orders')).toContain('`analytics_orders`.status');
        expect(await apiSql('pg_orders')).toContain('"pg_orders".status');
        expect(await apiSql('analytics_orders')).toContain('`analytics_orders`.status');
      } finally {
        api.dispose();
      }
    });

    // Without an external dialect, a query renders its external pre-aggregations in its
    // own dialect, so those templates must stay apart per data source
    test('renders external pre-aggregations of each data source in its own dialect', async () => {
      const api = new CompilerApi(
        {
          localPath: () => '/mock/path',
          dataSchemaFiles: () => Promise.resolve([{
            fileName: 'cubes.js',
            content: `
              cube('pg_orders', {
                sql: 'SELECT * FROM orders',
                measures: { count: { type: 'count' } },
                dimensions: { status: { sql: 'status', type: 'string' } },
                preAggregations: { byStatus: { measures: [CUBE.count], dimensions: [CUBE.status], external: true } },
              });
              cube('bq_orders', {
                sql: 'SELECT * FROM orders',
                dataSource: 'bigquery',
                measures: { count: { type: 'count' } },
                dimensions: { status: { sql: 'status', type: 'string' } },
                preAggregations: { byStatus: { measures: [CUBE.count], dimensions: [CUBE.status], external: true } },
              });
            `
          }]),
        },
        async ({ dataSource }) => (dataSource === 'bigquery' ? 'bigquery' : 'postgres'),
        {
          logger: () => {}, // eslint-disable-line @typescript-eslint/no-empty-function
          sqlCache: false,
        }
      );
      const apiSql = async (cube: string) => {
        const result = await api.getSql(
          { measures: [`${cube}.count`], dimensions: [`${cube}.status`], timezone: 'UTC' } as any
        );
        expect(result.preAggregations.map((p: any) => p.preAggregationId)).toEqual([`${cube}.byStatus`]);
        return result.sql[0];
      };

      try {
        expect(await apiSql('pg_orders')).toContain('"pg_orders__status"');
        expect(await apiSql('bq_orders')).toContain('`bq_orders__status`');
      } finally {
        api.dispose();
      }
    });
  });

  describe('getSql', () => {
    const repository: SchemaFileRepository = {
      localPath: () => '/mock/path',
      dataSchemaFiles: () => Promise.resolve([
        {
          fileName: 'visits.js',
          content: `
            cube('visits', {
              sql: "SELECT 1 AS id, 'a' AS source, 'b' AS browser",
              measures: { count: { type: 'count' } },
              dimensions: {
                id: { sql: 'id', type: 'number', primaryKey: true },
                source: { sql: 'source', type: 'string' },
                browser: { sql: 'browser', type: 'string' },
              },
              preAggregations: {
                bySource: { measures: [CUBE.count], dimensions: [CUBE.source] },
              },
            });
          `
        }
      ])
    };

    let compilerApi: CompilerApi;
    let matchOnlyRun: jest.SpyInstance;

    beforeEach(() => {
      compilerApi = new CompilerApi(repository, async () => 'postgres', {
        logger: () => {}, // eslint-disable-line @typescript-eslint/no-empty-function
        externalDbType: 'cubestore',
      });
      matchOnlyRun = jest.spyOn(BaseQuery.prototype, 'findPreAggregationForQueryRust');
    });

    afterEach(() => {
      matchOnlyRun.mockRestore();
      compilerApi.dispose();
    });

    const getSql = (dimension: string, options = {}) => compilerApi.getSql(
      { measures: ['visits.count'], dimensions: [dimension], timezone: 'UTC' } as any,
      options,
    );

    // Tesseract matches pre-aggregations while building the SQL, so a separate match-only
    // planner run would only repeat that work
    test('takes the pre-aggregation match from the build on a hit', async () => {
      const result = await getSql('visits.source');

      expect(result.external).toEqual(true);
      expect(result.preAggregations.map((p: any) => p.preAggregationId)).toEqual(['visits.bySource']);
      expect(matchOnlyRun).not.toHaveBeenCalled();
    });

    test('takes the pre-aggregation match from the build on a miss', async () => {
      const result = await getSql('visits.browser');

      expect(result.external).toEqual(false);
      expect(result.preAggregations).toEqual([]);
      expect(matchOnlyRun).not.toHaveBeenCalled();
    });

    test('renders the external pre-aggregation and the data source in their own dialects', async () => {
      const filtered = (dimension: string) => compilerApi.getSql({
        measures: ['visits.count'],
        dimensions: [dimension],
        filters: [{ member: dimension, operator: 'equals', values: ['a'] }],
        timezone: 'UTC',
      } as any);

      const hit = (await filtered('visits.source')).sql[0];
      const miss = (await filtered('visits.browser')).sql[0];

      expect(hit).toContain('= ?');
      expect(miss).toContain('= $1');
      expect((await filtered('visits.source')).sql[0]).toEqual(hit);
    });

    test('fetches the templates of each dialect once, pre-aggregation SQL included', async () => {
      const sqlTemplates = jest.spyOn(BaseQuery.prototype, 'sqlTemplates');
      try {
        const result = await getSql('visits.source');

        expect(result.preAggregations[0].loadSql[0]).toContain('"visits".source');
        expect(sqlTemplates.mock.contexts.map((query: any) => query.constructor.name).sort())
          .toEqual(['CubeStoreQuery', 'PostgresQuery']);
      } finally {
        sqlTemplates.mockRestore();
      }
    });

    test('matches once without a build for preAggregationsOnly', async () => {
      const result = await getSql('visits.source', { preAggregationsOnly: true });

      expect(result.sql).toBeNull();
      expect(result.preAggregations.map((p: any) => p.preAggregationId)).toEqual(['visits.bySource']);
      expect(matchOnlyRun).toHaveBeenCalledTimes(1);
    });
  });

  describe('getSqlGenerator data source', () => {
    // `analytics` has its own dbType, so it needs its own dialect
    const model = `
      cube('pg_orders', {
        sql: 'SELECT * FROM orders',
        measures: { count: { type: 'count' } },
        dimensions: {
          id: { sql: 'id', type: 'number', primaryKey: true },
          status: { sql: 'status', type: 'string' },
        },
        preAggregations: {
          byStatus: { measures: [CUBE.count], dimensions: [CUBE.status], external: true },
          joined: {
            type: 'rollupJoin',
            measures: [CUBE.count],
            dimensions: [bq_customers.name],
            rollups: [pg_orders.byStatus, bq_customers.byName],
            external: true,
          },
        },
        joins: {
          bq_customers: { relationship: 'many_to_one', sql: \`\${CUBE.status} = \${bq_customers.name}\` },
        },
      });

      cube('bq_customers', {
        sql: 'SELECT * FROM customers',
        dataSource: 'analytics',
        measures: { count: { type: 'count' } },
        dimensions: {
          id: { sql: 'id', type: 'number', primaryKey: true },
          name: { sql: 'name', type: 'string' },
        },
        preAggregations: {
          byName: { dimensions: [CUBE.name], external: true },
        },
      });

      cube('bq_orders', {
        sql: 'SELECT * FROM orders',
        dataSource: 'analytics',
        measures: { count: { type: 'count' } },
        dimensions: {
          id: { sql: 'id', type: 'number', primaryKey: true },
          customer_id: { sql: 'customer_id', type: 'number' },
          status: { sql: 'status', type: 'string' },
          customer_name: { sql: \`\${bq_customers.name}\`, type: 'string' },
        },
        joins: {
          bq_customers: { relationship: 'many_to_one', sql: \`\${CUBE}.customer_id = \${bq_customers.id}\` },
        },
      });

      cube('bq_masked', {
        sql: 'SELECT * FROM masked',
        dataSource: 'analytics',
        dimensions: {
          id: { sql: 'id', type: 'number', primaryKey: true },
          status: { sql: \`\${pg_orders.status}\`, type: 'string', mask: 'hidden' },
        },
      });

      view('bq_view', {
        cubes: [
          { join_path: bq_orders, includes: ['count', 'status'] },
          { join_path: bq_orders.bq_customers, prefix: true, includes: ['name'] },
        ],
      });
    `;

    let compilerApi: CompilerApi;
    let builds: jest.SpyInstance;

    beforeEach(() => {
      compilerApi = new CompilerApi(
        {
          localPath: () => '/mock/path',
          dataSchemaFiles: () => Promise.resolve([{ fileName: 'model.js', content: model }]),
        },
        async ({ dataSource }) => (dataSource === 'analytics' ? 'bigquery' : 'postgres'),
        {
          logger: () => {}, // eslint-disable-line @typescript-eslint/no-empty-function
          externalDbType: 'cubestore',
          sqlCache: false,
        }
      );
      builds = jest.spyOn(compilerApi, 'createQueryByDataSource');
    });

    afterEach(() => {
      builds.mockRestore();
      compilerApi.dispose();
    });

    const generate = async (query: Record<string, unknown>) => {
      const { sqlGenerator, compilers } = await compilerApi.getSqlGenerator({ timezone: 'UTC', ...query } as any);
      const [sql] = compilers.compiler.withQuery(sqlGenerator, () => sqlGenerator.buildSqlAndParams());
      const dataSource = compilers.compiler.withQuery(sqlGenerator, () => sqlGenerator.dataSource);
      return { sqlGenerator, sql, dataSource };
    };

    // Built straight in the right dialect, so it plans the query itself
    const generateWithoutPlan = async (query: Record<string, unknown>, dataSource: string) => {
      const compilers = await compilerApi.getCompilers();
      const sqlGenerator = await compilerApi.createQueryByDataSource(compilers, { timezone: 'UTC', ...query }, dataSource);
      const [sql] = compilers.compiler.withQuery(sqlGenerator, () => sqlGenerator.buildSqlAndParams());
      return sql;
    };

    const builtDataSources = () => builds.mock.calls.map(([, , dataSource]) => dataSource);

    describe.each([true, false])('useNativeSqlPlanner: %s', (useNativeSqlPlanner) => {
      test.each([
        ['member SQL that references another cube', {
          measures: ['bq_orders.count'],
          dimensions: ['bq_orders.customer_name'],
        }],
        ['a view', {
          measures: ['bq_view.count'],
          dimensions: ['bq_view.bq_customers_name'],
        }],
        ['a cube it only filters by', {
          measures: ['bq_orders.count'],
          filters: [{
            or: [
              { member: 'bq_customers.name', operator: 'equals', values: ['a'] },
              { member: 'bq_orders.status', operator: 'set' },
            ],
          }],
        }],
      ])('builds a query on %s once, in its data source\'s dialect from the plan', async (_, query) => {
        const { sqlGenerator, sql, dataSource } = await generate({ ...query, useNativeSqlPlanner });

        expect(sqlGenerator.constructor.name).toEqual('BigqueryQuery');
        expect(dataSource).toEqual('analytics');
        expect(builtDataSources()).toEqual(['analytics']);
        expect(builds.mock.calls[0][1].plan.dataSource).toEqual('analytics');
        expect(sql).toEqual(await generateWithoutPlan({ ...query, useNativeSqlPlanner }, 'analytics'));
      });
    });

    test('looks up the dbType of the planned data source only', async () => {
      // Compiling looks up the dbType of every data source, once
      await compilerApi.getCompilers();
      const dbTypes = jest.spyOn(compilerApi, 'getDbType');
      await compilerApi.getSqlGenerator({ measures: ['bq_orders.count'] } as any);

      expect(dbTypes.mock.calls).toEqual([['analytics']]);
    });

    test('builds a query on a masked member, which reads only its own cube, in its dialect', async () => {
      const { sqlGenerator } = await compilerApi.getSqlGenerator({
        dimensions: ['bq_masked.status'],
        maskedMembers: [{ member: 'bq_masked.status' }],
      } as any);

      expect(sqlGenerator.constructor.name).toEqual('BigqueryQuery');
      expect(builtDataSources()).toEqual(['analytics']);
    });

    test('builds a rollupJoin query across data sources once, in the first one\'s dialect', async () => {
      const { sqlGenerator, sql, dataSource } = await generate({
        measures: ['pg_orders.count'],
        dimensions: ['bq_customers.name'],
      });

      expect(sqlGenerator.constructor.name).toEqual('PostgresQuery');
      expect(dataSource).toEqual('default');
      expect(sql).toContain('pg_orders_by_status');
      expect(builtDataSources()).toEqual(['default']);
    });

    test('builds a query without members in the default data source', async () => {
      const { sqlGenerator } = await compilerApi.getSqlGenerator({} as any);

      expect(sqlGenerator.constructor.name).toEqual('PostgresQuery');
      expect(builtDataSources()).toEqual(['default']);
    });

    // The api-gateway builds those to initialize the SQL API of each data source
    test('builds a query without members in the given data source', async () => {
      const compilers = await compilerApi.getCompilers();
      const sqlGenerator = await compilerApi.createQueryByDataSource(compilers, {}, 'analytics');

      expect(sqlGenerator.constructor.name).toEqual('BigqueryQuery');
    });

    // The rollup's own query is built in its cube's data source, which the joined cube is not in
    test('rejects a rollup that joins a cube of another data source', async () => {
      const api = new CompilerApi(
        {
          localPath: () => '/mock/path',
          dataSchemaFiles: () => Promise.resolve([{
            fileName: 'cubes.js',
            content: `
              cube('orders', {
                sql: 'SELECT * FROM orders',
                joins: { users: { relationship: 'many_to_one', sql: \`\${CUBE}.user_id = \${users}.id\` } },
                measures: { count: { type: 'count' } },
                dimensions: {
                  id: { sql: 'id', type: 'number', primaryKey: true },
                  user_id: { sql: 'user_id', type: 'number' },
                },
                preAggregations: { byCity: { measures: [CUBE.count], dimensions: [users.city], external: true } },
              });
              cube('users', {
                sql: 'SELECT * FROM users',
                dataSource: 'crm',
                measures: { count: { type: 'count' } },
                dimensions: {
                  id: { sql: 'id', type: 'number', primaryKey: true },
                  city: { sql: 'city', type: 'string' },
                },
              });
            `
          }]),
        },
        async () => 'postgres',
        {
          logger: () => {}, // eslint-disable-line @typescript-eslint/no-empty-function
          externalDbType: 'cubestore',
        }
      );

      try {
        await expect(api.getSql(
          { measures: ['orders.count'], dimensions: ['users.city'], preAggregationId: 'orders.byCity', timezone: 'UTC' } as any,
          { preAggregationsOnly: true },
        )).rejects.toThrow('To join across data sources use rollupJoin with Cube Store');
      } finally {
        api.dispose();
      }
    });
  });

  // BaseQuery throws for these, so planning without a dialect must not reach them
  describe('getSqlGenerator over dialect functions BaseQuery lacks', () => {
    const model = `
      cube('orders', {
        sql: 'SELECT * FROM orders',
        measures: {
          count: { type: 'count' },
          approx_users: { sql: 'user_id', type: 'count_distinct_approx' },
        },
        dimensions: {
          id: { sql: 'id', type: 'number', primaryKey: true },
          created_at: { sql: 'created_at', type: 'time' },
        },
      });
    `;

    let compilerApi: CompilerApi;

    beforeEach(() => {
      compilerApi = new CompilerApi(
        {
          localPath: () => '/mock/path',
          dataSchemaFiles: () => Promise.resolve([{ fileName: 'model.js', content: model }]),
        },
        async () => 'postgres',
        {
          logger: () => {}, // eslint-disable-line @typescript-eslint/no-empty-function
          sqlCache: false,
        }
      );
    });

    afterEach(() => {
      compilerApi.dispose();
    });

    test.each([
      ['an approximate distinct count', { measures: ['orders.approx_users'] }],
      // As the SQL API pushes down `date_trunc('day', orders.created_at)`
      ['a member expression over a time granularity', {
        measures: ['orders.count'],
        dimensions: [{
          cubeName: 'orders',
          name: 'created_at_day',
          expressionName: 'created_at_day',
          expression: (orders: any) => `${orders.created_at.day}`,
          definition: 'created_at.day',
          cubeParams: ['orders'],
        }],
      }],
    ])('builds a query on %s', async (_, query) => {
      const { sqlGenerator, compilers } = await compilerApi.getSqlGenerator({ timezone: 'UTC', ...query } as any);
      const [sql] = compilers.compiler.withQuery(sqlGenerator, () => sqlGenerator.buildSqlAndParams());

      expect(sqlGenerator.constructor.name).toEqual('PostgresQuery');
      expect(sql).toContain('orders');
    });
  });

  describe('createQueryFactory', () => {
    const repository: SchemaFileRepository = {
      localPath: () => '/mock/path',
      dataSchemaFiles: () => Promise.resolve([
        {
          fileName: 'model.js',
          content: `
            cube('orders', {
              sql: 'SELECT * FROM orders',
              measures: { count: { type: 'count' } },
            });
            cube('customers', {
              sql: 'SELECT * FROM customers',
              measures: { count: { type: 'count' } },
            });
            cube('events', {
              sql: 'SELECT * FROM events',
              dataSource: 'events',
              measures: { count: { type: 'count' } },
            });
            cube('sessions', {
              sql: 'SELECT * FROM sessions',
              dataSource: 'events',
              measures: { count: { type: 'count' } },
            });
          `
        }
      ])
    };

    test('resolves the dialect once per data source', async () => {
      const dbType = jest.fn(async ({ dataSource }) => (dataSource === 'events' ? 'mysql' : 'postgres'));
      const compilerApi = new CompilerApi(repository, dbType, {
        logger: () => {}, // eslint-disable-line @typescript-eslint/no-empty-function
      });

      try {
        const compilers = await compilerApi.getCompilers();
        dbType.mockClear();

        const queryFactory = await compilerApi.createQueryFactory(compilers);
        expect(dbType.mock.calls.map(([ctx]) => ctx.dataSource).sort()).toEqual(['default', 'events']);

        expect(queryFactory.createQuery('orders', compilers, {})).toBeInstanceOf(PostgresQuery);
        expect(queryFactory.createQuery('sessions', compilers, {})).toBeInstanceOf(MysqlQuery);
      } finally {
        compilerApi.dispose();
      }
    });
  });
});

describe('CompilerApi externalPreAggregationMembers', () => {
  // cubesql plans a scan over a Cube Store rollup only when the scan's members fit one
  // of these sets, so each external rollup has to show up with every member it holds,
  // rollup joins expanded, members reading those (even through others) included, and
  // views spelled in their own member names. `amount` reads none of them.
  const model = `
cubes:
  - name: orders
    sql: SELECT * FROM orders
    joins:
      - name: customers
        sql: "{CUBE}.customer_id = {customers}.id"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: status
        sql: status
        type: string
      - name: created_at
        sql: created_at
        type: time
      - name: status_label
        sql: "CONCAT({CUBE.status}, '!')"
        type: string
    measures:
      - name: count
        type: count
      - name: amount
        sql: amount
        type: sum
      - name: double_count
        sql: "{CUBE.count} * 2"
        type: number
      - name: double_count_share
        sql: "{CUBE.double_count} / 2"
        type: number
    pre_aggregations:
      - name: main
        external: true
        dimensions:
          - status
          - customers.gender
        measures:
          - count
        time_dimension: created_at
        granularity: day
      - name: internal
        external: false
        dimensions:
          - status
  - name: customers
    sql: SELECT * FROM customers
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: gender
        sql: gender
        type: string
    measures:
      - name: count
        type: count

views:
  - name: orders_view
    cubes:
      - join_path: orders
        includes:
          - status
          - count
      - join_path: orders.customers
        prefix: true
        includes:
          - gender
`;

  test('lists the members of every external rollup, views in their own names', async () => {
    const compilerApi = new CompilerApi(
      {
        localPath: () => '/mock/path',
        dataSchemaFiles: () => Promise.resolve([{ fileName: 'orders.yml', content: model }]),
      },
      async () => 'postgres',
      {
        logger: () => {}, // eslint-disable-line @typescript-eslint/no-empty-function
      }
    );

    try {
      const rollups = (await compilerApi.externalPreAggregationMembers())
        .map((members) => [...members].sort());

      expect(rollups).toEqual([
        [
          'customers.gender', 'orders.count', 'orders.created_at', 'orders.double_count',
          'orders.double_count_share', 'orders.status', 'orders.status_label',
        ],
        ['orders_view.count', 'orders_view.customers_gender', 'orders_view.status'],
      ]);
    } finally {
      compilerApi.dispose();
    }
  });
});
