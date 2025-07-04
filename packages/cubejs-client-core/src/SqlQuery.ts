export type SqlQueryTuple = [string, any[], any?];

export type SqlData = {
  aliasNameToMember: Record<string, string>;
  cacheKeyQueries: SqlQueryTuple[];
  dataSource: string;
  external: boolean;
  sql: SqlQueryTuple;
  preAggregations: any[];
  rollupMatchResults: any[];
};

export type SqlQueryWrapper = { sql: SqlData };

export default class SqlQuery {
  private readonly sqlQuery: SqlQueryWrapper;

  public constructor(sqlQuery: SqlQueryWrapper) {
    this.sqlQuery = sqlQuery;
  }

  public rawQuery(): SqlData {
    return this.sqlQuery.sql;
  }

  public sql(): string {
    return this.rawQuery().sql[0];
  }
}
