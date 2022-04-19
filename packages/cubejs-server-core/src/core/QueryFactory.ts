import { createQuery } from '@cubejs-backend/schema-compiler';
import { CompilerApi } from './CompilerApi';

export class QueryFactory {
  public static async create(api: CompilerApi, compilers: any) {
    const { cubeEvaluator } = compilers;
    const cubeToDbType = {};
    const cubeToDialectClass = {};
    for (const cube of cubeEvaluator.cubeNames()) {
      const { dataSource } = cubeEvaluator.cubeFromPath(cube);
      const dbType = api.getDbType(dataSource);
      const dialectClass = api.getDialectClass(dataSource, dbType);
      cubeToDbType[cube] = dbType;
      cubeToDialectClass[cube] = dialectClass;
    }
    return new QueryFactory(compilers, cubeToDbType, cubeToDialectClass);
  }

  private constructor(
    private compilers: any,
    private cubeToDbType: Record<string, string>,
    private cubeToDialectClass: Record<string, any>,
  ) {
  }

  public createQuery(cube: string, queryOptions: any) {
    if (!(cube in this.cubeToDbType)) {
      throw new Error(`${cube}: undefined dbType`);
    }
    const dbType = this.cubeToDbType[cube];
    const dialectClass = this.cubeToDialectClass[cube];
    return createQuery(this.compilers, dbType, { ...queryOptions, dialectClass });
  }
}
