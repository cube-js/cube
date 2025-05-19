import type { BaseQuery } from '../../../src';
import { dbRunner } from './PostgresDBRunner';

export type QueryWithParams = [string, Array<unknown>];

export async function testWithPreAggregation(
  preAggregationsDescription: { loadSql: QueryWithParams, invalidateKeyQueries: Array<QueryWithParams> },
  query: BaseQuery,
) {
  const preAggSql = preAggregationsDescription
    .loadSql[0]
    // Without `ON COMMIT DROP` temp tables are session-bound, and can live across multiple transactions
    .replace(/CREATE TABLE (.+) AS SELECT/, 'CREATE TEMP TABLE $1 ON COMMIT DROP AS SELECT');
  const preAggParams = preAggregationsDescription.loadSql[1];

  const queries = [
    ...preAggregationsDescription.invalidateKeyQueries,
    [preAggSql, preAggParams],
    query.buildSqlAndParams(),
  ];

  return dbRunner.testQueries(queries);
}
