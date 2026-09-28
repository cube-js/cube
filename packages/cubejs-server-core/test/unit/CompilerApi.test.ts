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
});
