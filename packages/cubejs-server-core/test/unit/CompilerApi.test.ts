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

  public leafFilterImplies(queryLeaf: any, targetLeaf: any): boolean {
    return super.leafFilterImplies(queryLeaf, targetLeaf);
  }

  public queryFiltersImplyFilter(queryFilters: any[], maskFilter: any): boolean {
    return super.queryFiltersImplyFilter(queryFilters, maskFilter);
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

    test('should drop cached GraphQL schemas on dispose', async () => {
      const mockSchema = {} as any;
      compilerApi.setGraphQLSchema('compiler-id', mockSchema);

      expect(compilerApi.getGraphQLSchema('compiler-id')).toBe(mockSchema);
      expect(compilerApi.getGraphQLSchema('other-compiler-id')).toBeUndefined();

      compilerApi.dispose();

      expect(compilerApi.getGraphQLSchema('compiler-id')).toBeUndefined();
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
});

describe('CompilerApi leafFilterImplies', () => {
  const repository: SchemaFileRepository = {
    localPath: () => '/mock/path',
    dataSchemaFiles: () => Promise.resolve([]),
  };
  const compilerApi = new CompilerApiTestable(repository, async () => 'postgres', {
    logger: () => {}, // eslint-disable-line @typescript-eslint/no-empty-function
  });

  afterAll(() => compilerApi.dispose());

  const leaf = (operator: string, values: (string | number)[]) => ({ member: 'statements.booked', operator, values });

  test('a range filter with reversed endpoints does not imply the policy range', () => {
    for (const operator of ['inDateRange', 'notInDateRange']) {
      expect(compilerApi.leafFilterImplies(
        leaf(operator, ['2025-03-31', '2025-02-01']),
        leaf(operator, ['2025-02-01', '2025-03-31']),
      )).toBe(false);
      expect(compilerApi.leafFilterImplies(
        leaf(operator, ['2025-02-01', '2025-03-31']),
        leaf(operator, ['2025-02-01', '2025-03-31']),
      )).toBe(true);
    }
  });

  test('date conditions only match the policy byte for byte', () => {
    const policy = leaf('inDateRange', ['2025-02-01', '2025-03-31']);
    const notImplied: [string, any[]][] = [
      ['inDateRange', ['2025-02-10', '2025-02-20']],
      ['inDateRange', ['2025-02-01T00:00:00.000', '2025-03-31T23:59:59.999']],
      ['inDateRange', ['2025-02-01T00:00:00.000Z', '2025-03-31T23:59:59.999Z']],
      ['inDateRange', ['2025-02-01', '2025-03-31', '2025-04-01']],
      ['inDateRange', ['2025-02-01']],
      ['notInDateRange', ['2025-02-01', '2025-03-31']],
      ['onTheDate', ['2025-02-01', '2025-03-31']],
      ['equals', ['2025-02-01', '2025-03-31']],
    ];

    for (const [operator, values] of notImplied) {
      expect(compilerApi.leafFilterImplies(leaf(operator, values), policy)).toBe(false);
    }

    expect(compilerApi.leafFilterImplies(leaf('afterDate', ['2025-03-01']), leaf('afterDate', ['2025-02-01']))).toBe(false);
    expect(compilerApi.leafFilterImplies(leaf('beforeDate', ['2025-02-01']), leaf('beforeOrOnDate', ['2025-02-01']))).toBe(false);
    expect(compilerApi.leafFilterImplies(leaf('gt', ['2025-03-01']), leaf('gt', ['2025-02-01']))).toBe(false);
    expect(compilerApi.leafFilterImplies(leaf('afterDate', ['2025-02-01']), leaf('afterDate', ['2025-02-01']))).toBe(true);
  });

  test('null is not the string "null"', () => {
    expect(compilerApi.leafFilterImplies(leaf('equals', ['null']), leaf('equals', [null] as any))).toBe(false);
    expect(compilerApi.leafFilterImplies(leaf('equals', ['null']), leaf('equals', ['a', null] as any))).toBe(false);
    expect(compilerApi.leafFilterImplies(leaf('notEquals', ['a', 'null']), leaf('notEquals', ['a', null] as any))).toBe(false);
    expect(compilerApi.leafFilterImplies(
      leaf('inDateRange', ['null', 'null']),
      leaf('inDateRange', [null, null] as any),
    )).toBe(false);
    expect(compilerApi.leafFilterImplies(leaf('equals', ['undefined']), leaf('equals', [undefined] as any))).toBe(false);
    expect(compilerApi.leafFilterImplies(leaf('equals', [['a', 'b']] as any), leaf('equals', ['a,b']))).toBe(false);
  });

  test('values that are not an array never imply', () => {
    expect(compilerApi.leafFilterImplies(
      leaf('inDateRange', ['2025-02-01', '2025-03-31']),
      { member: 'statements.booked', operator: 'inDateRange', values: 'last month' },
    )).toBe(false);
  });

  test('an empty policy group does not unmask', () => {
    const queryFilters = [leaf('equals', ['a'])];
    expect(compilerApi.queryFiltersImplyFilter(queryFilters, { and: [] })).toBe(false);
    expect(compilerApi.queryFiltersImplyFilter(queryFilters, { or: [] })).toBe(false);
    expect(compilerApi.queryFiltersImplyFilter(queryFilters, {})).toBe(false);
    expect(compilerApi.queryFiltersImplyFilter([{ or: queryFilters }], leaf('equals', ['a']))).toBe(false);
    expect(compilerApi.queryFiltersImplyFilter([{ and: queryFilters }], leaf('equals', ['a']))).toBe(true);
  });

  test('set operators still match regardless of value order', () => {
    expect(compilerApi.leafFilterImplies(leaf('equals', ['b', 'a']), leaf('equals', ['a', 'b']))).toBe(true);
    expect(compilerApi.leafFilterImplies(leaf('notEquals', ['b', 'a']), leaf('notEquals', ['a', 'b']))).toBe(true);
  });
});

describe('CompilerApi conditional mask lifting', () => {
  const model = `
cubes:
  - name: orders
    sql_table: orders
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: created_at
        sql: created_at
        type: time
    measures:
      - name: total
        sql: amount
        type: sum
      - name: total_prev_month
        multi_stage: true
        sql: "{total}"
        type: number
        time_shift:
          - time_dimension: created_at
            interval: 1 month
            type: prior
      - name: total_prev_month_doubled
        multi_stage: true
        sql: "{total_prev_month} * 2"
        type: number
    access_policy:
      - group: "*"
        member_level:
          includes: []
        member_masking:
          includes: "*"
      - group: dates
        member_level:
          includes: "*"
        row_level:
          filters:
            - member: created_at
              operator: inDateRange
              values: ["2025-02-01", "2025-03-31"]
`;
  const repository: SchemaFileRepository = {
    localPath: () => '/mock/path',
    dataSchemaFiles: () => Promise.resolve([{ fileName: 'model.yml', content: model }]),
  };
  const compilerApi = new CompilerApi(repository, async () => 'postgres', {
    logger: () => {}, // eslint-disable-line @typescript-eslint/no-empty-function
    contextToGroups: async (context: any) => context.securityContext.groups,
  });

  afterAll(() => compilerApi.dispose());

  const maskedMembers = async (group: string, measure: string, filter: any) => {
    const query: any = { timezone: 'UTC', measures: [`orders.${measure}`], filters: [filter] };
    const context: any = { securityContext: { groups: [group] }, requestId: 'test' };
    const result = await compilerApi.applyRowLevelSecurity(query, query, context);
    return ((result.query as any).maskedMembers || []).map((m: any) => m.member);
  };

  const policyRange = { member: 'orders.created_at', operator: 'inDateRange', values: ['2025-02-01', '2025-03-31'] };

  test('the policy filter unmasks a measure that reads the filtered rows', async () => {
    expect(await maskedMembers('dates', 'total', policyRange)).toEqual([]);
  });

  test('a time-shifted measure stays masked, also behind another measure', async () => {
    expect(await maskedMembers('dates', 'total_prev_month', policyRange)).toContain('orders.total_prev_month');
    expect(await maskedMembers('dates', 'total_prev_month_doubled', policyRange))
      .toContain('orders.total_prev_month_doubled');
  });
});
