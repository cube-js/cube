import { SchemaFileRepository } from '@cubejs-backend/shared';
import { BaseQuery, MysqlQuery, PostgresQuery } from '@cubejs-backend/schema-compiler';
import type { Compiler, QueryFactory } from '@cubejs-backend/schema-compiler';
import { CompilerApi } from '../../src/core/CompilerApi';
import { DbTypeInternalFn } from '../../src/core/types';

// Test helper class to expose protected properties
class CompilerApiTestable extends CompilerApi {
  public getCompilersProperty(): Promise<Compiler> | any {
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

    test('matches once without a build for preAggregationsOnly', async () => {
      const result = await getSql('visits.source', { preAggregationsOnly: true });

      expect(result.sql).toBeNull();
      expect(result.preAggregations.map((p: any) => p.preAggregationId)).toEqual(['visits.bySource']);
      expect(matchOnlyRun).toHaveBeenCalledTimes(1);
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
