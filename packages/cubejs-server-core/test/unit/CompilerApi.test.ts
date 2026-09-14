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

  describe('applyRowLevelSecurity', () => {
    let compilerApi: CompilerApi;

    // `analyst` may read `count` and `status`; `secret` and `other_secret` are
    // granted by no policy, so querying them is a member-level denial. `id` is
    // granted too: policies are checked against the members the generated SQL
    // references, which includes the cube's primary key.
    const rbacRepository: SchemaFileRepository = {
      localPath: () => '/mock/path',
      dataSchemaFiles: () => Promise.resolve([
        {
          fileName: 'orders.js',
          content: `
            cube('orders', {
              sql: 'SELECT * FROM orders',
              measures: {
                count: {
                  type: 'count'
                }
              },
              dimensions: {
                id: {
                  sql: 'id',
                  type: 'number',
                  primaryKey: true
                },
                status: {
                  sql: 'status',
                  type: 'string'
                },
                secret: {
                  sql: 'secret',
                  type: 'string'
                },
                other_secret: {
                  sql: 'other_secret',
                  type: 'string'
                }
              },
              accessPolicy: [
                {
                  group: 'analyst',
                  memberLevel: {
                    includes: ['count', 'status', 'id']
                  }
                }
              ]
            });
          `
        }
      ])
    };

    const analystContext = {
      requestId: 'test-request',
      securityContext: { groups: ['analyst'] },
    };

    beforeEach(() => {
      compilerApi = new CompilerApi(
        rbacRepository,
        async () => 'postgres',
        {
          logger: () => {}, // eslint-disable-line @typescript-eslint/no-empty-function
          contextToGroups: (context: any) => context.securityContext?.groups || [],
        }
      );
    });

    afterEach(() => {
      if (compilerApi) {
        compilerApi.dispose();
      }
    });

    test('allows a query that every member is granted for', async () => {
      const query: any = {
        measures: ['orders.count'],
        dimensions: ['orders.status'],
        timezone: 'UTC',
        filters: [],
      };

      const result = await compilerApi.applyRowLevelSecurity(query, query, analystContext);

      expect(result.denied).toBe(false);
      expect(result.deniedMembers).toEqual([]);
    });

    test('denies a query and reports every member no policy grants', async () => {
      const query: any = {
        measures: ['orders.count'],
        dimensions: ['orders.secret', 'orders.other_secret'],
        timezone: 'UTC',
        filters: [],
      };

      const result = await compilerApi.applyRowLevelSecurity(query, query, analystContext);

      expect(result.denied).toBe(true);
      // Both denied members are reported, not just the first one encountered
      expect(result.deniedMembers).toEqual(
        expect.arrayContaining(['orders.secret', 'orders.other_secret'])
      );
      // Granted members are not reported as denied
      expect(result.deniedMembers).not.toContain('orders.count');
      // The query is neutralized so an API that keeps serving it returns no rows
      expect(result.query.segments).toContainEqual(
        expect.objectContaining({ name: 'rlsAccessDenied', cubeName: 'orders' })
      );
    });

    test('denies every member when no policy matches the user at all', async () => {
      const query: any = {
        measures: ['orders.count'],
        dimensions: ['orders.status'],
        timezone: 'UTC',
        filters: [],
      };

      const result = await compilerApi.applyRowLevelSecurity(query, query, {
        requestId: 'test-request',
        securityContext: { groups: ['nobody'] },
      });

      expect(result.denied).toBe(true);
      expect(result.deniedMembers).toEqual(
        expect.arrayContaining(['orders.count', 'orders.status'])
      );
    });
  });
});
