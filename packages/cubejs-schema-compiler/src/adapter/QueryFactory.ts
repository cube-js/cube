import type { CubeEvaluator } from '../compiler/CubeEvaluator';

export class QueryFactory {
  public constructor(
    private dataSourceToQueryClass: Record<string, any>,
  ) {
  }

  public queryClassForDataSource(dataSource: string) {
    const QueryClass = this.dataSourceToQueryClass[dataSource];
    if (!QueryClass) {
      throw new Error(`Undefined dbType or dialectClass for '${dataSource}' data source`);
    }
    return QueryClass;
  }

  public createQuery(cube: string, compilers: { cubeEvaluator: CubeEvaluator }, queryOptions: any) {
    const QueryClass = this.queryClassForDataSource(compilers.cubeEvaluator.cubeDataSource(cube));
    return new QueryClass(compilers, queryOptions);
  }
}
