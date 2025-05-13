export default class SqlQuery {
  constructor(sqlQuery) {
    this.sqlQuery = sqlQuery;
  }

  rawQuery() {
    return this.sqlQuery.sql;
  }

  sql() {
    return this.rawQuery().sql[0];
  }
}
