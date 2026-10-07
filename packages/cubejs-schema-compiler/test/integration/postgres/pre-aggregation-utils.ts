import type { BaseQuery } from '../../../src';
import { dbRunner } from './PostgresDBRunner';

export type QueryWithParams = [string, Array<unknown>];

export async function testWithPreAggregation(
  preAggregationsDescription: { tableName: string, loadSql: QueryWithParams, invalidateKeyQueries: Array<QueryWithParams> },
  query: BaseQuery,
) {
  const preAggSql = preAggregationsDescription
    .loadSql[0]
    // Without `ON COMMIT DROP` temp tables are session-bound, and can live across multiple transactions
    .replace(/CREATE TABLE (.+) AS\s+(SELECT|WITH)/, 'CREATE TEMP TABLE $1 ON COMMIT DROP AS $2');
  const preAggParams = preAggregationsDescription.loadSql[1];

  // Usages read the table under a `__usage_N` name the orchestrator maps back.
  const [querySql, queryParams] = query.buildSqlAndParams();
  const usageTableName = new RegExp(`${preAggregationsDescription.tableName}__usage_\\d+`, 'g');

  const queries = [
    ...preAggregationsDescription.invalidateKeyQueries,
    [preAggSql, preAggParams],
    [querySql.replace(usageTableName, preAggregationsDescription.tableName), queryParams],
  ];

  return dbRunner.testQueries(queries);
}
