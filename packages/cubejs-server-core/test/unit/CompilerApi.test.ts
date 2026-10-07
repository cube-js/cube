import { SchemaFileRepository } from '@cubejs-backend/shared';
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

    const sqlInTwoMinutes = async (compilerApi: CompilerApi) => {
      const query: any = { measures: ['orders.amount_in_range'], timezone: 'UTC' };
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

    test('absolute date ranges keep the cached SQL', async () => {
      const { first, nextMinute } = await sqlInTwoMinutes(compilerApiFor('["2026-10-01", "2026-10-07"]'));
      expect(nextMinute).toBe(first);
    });
  });
});
