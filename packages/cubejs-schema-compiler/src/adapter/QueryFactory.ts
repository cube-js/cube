import type { BaseQuery, BaseQueryOptions } from './BaseQuery';
import type { Compilers } from '../compiler/PrepareCompiler';

export class QueryFactory {
  public constructor(
    protected dataSourceToQueryClass: Record<string, typeof BaseQuery>,
  ) {
  }

  protected queryClassForDataSource(dataSource: string) {
    const QueryClass = this.dataSourceToQueryClass[dataSource];
    if (!QueryClass) {
      throw new Error(`Undefined dbType or dialectClass for '${dataSource}' data source`);
    }
    return QueryClass;
  }

  public createQuery(cube: string, compilers: Compilers, queryOptions: BaseQueryOptions): BaseQuery {
    const dataSource = compilers.cubeEvaluator.cubeDataSource(cube);
    const QueryClass = this.queryClassForDataSource(dataSource);
    return compilers.createQuery(QueryClass, { ...queryOptions, dataSource });
  }
}
