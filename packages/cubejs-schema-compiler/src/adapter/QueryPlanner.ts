import { BaseQuery } from './BaseQuery';

/**
 * Plans a query before its dialect is known, see `plan()`. Planning renders member SQL only to
 * collect what it references, so the SQL it renders is never run. BaseQuery leaves some dialect
 * functions unimplemented and throws there, so they render their input as is.
 */
export class QueryPlanner extends BaseQuery {
  public hllInit(sql: string): string {
    return sql;
  }

  public hllMerge(sql: string): string {
    return sql;
  }

  public hllCardinality(sql: string): string {
    return sql;
  }

  public countDistinctApprox(sql: string): string {
    return `COUNT(DISTINCT ${sql})`;
  }

  public convertTz(field: string): string {
    return field;
  }

  public timeGroupedColumn(_granularity: string, dimension: string): string {
    return dimension;
  }

  public dateBin(_interval: string, source: string, _origin: string): string {
    return source;
  }
}
