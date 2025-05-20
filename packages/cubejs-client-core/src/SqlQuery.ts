export type SqlQueryTuple = [string, any[], any];

export type SqlData = {
  aliasNameToMember: Record<string, string>;
  cacheKeyQueries: SqlQueryTuple[];
  dataSource: boolean;
  external: boolean;
  sql: SqlQueryTuple;
  preAggregations: any[];
  rollupMatchResults: any[];
};

export default class SqlQuery {
  private readonly sqlQuery: SqlData;

  public constructor(sqlQuery: SqlData) {
    this.sqlQuery = sqlQuery;
  }

  public rawQuery(): SqlData {
    return this.sqlQuery;
  }

  public sql(): string {
    return this.rawQuery().sql[0];
  }
}
