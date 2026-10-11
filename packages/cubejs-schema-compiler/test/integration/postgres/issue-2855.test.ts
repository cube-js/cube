import { PreAggregationPartitionRangeLoader, PreAggregations, getStructureVersion } from '@cubejs-backend/query-orchestrator';
import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// https://github.com/cube-js/cube/issues/2855
// In-database (`external: false`) pre-aggregation tables on Postgres are named
// `<cube>_<pre_agg><partition>_<content_version>_<structure_version>_<last_updated_at>`.
// Postgres silently truncates identifiers to 63 bytes, so a cube + pre-aggregation name
// longer than ~29 chars yields tables whose stored name differs from what Cube expects:
// partitions collide (`relation ... already exists`) and the refresh fails with
// `Pre-aggregation table is not found for ... after it was successfully created`.
describe('Issue 2855: long pre-aggregation table names on Postgres', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: very_long_visitors_cube_name_for_issue
    sql: SELECT * FROM visitors
    measures:
      - name: amount
        sql: amount
        type: sum
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: created_at
        sql: created_at
        type: time
    pre_aggregations:
      - name: monthly_amount_rollup
        type: rollup
        external: false
        measures:
          - amount
        time_dimension: created_at
        granularity: day
        partition_granularity: month
`);

  it('every partition table is stored under the name Cube looks it up by', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['very_long_visitors_cube_name_for_issue.amount'],
      timeDimensions: [{
        dimension: 'very_long_visitors_cube_name_for_issue.created_at',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-02-28'],
      }],
      timezone: 'UTC',
      // `pg_temp` keeps the tables in the transaction's temp schema so nothing persists.
      preAggregationsSchema: 'pg_temp',
    });

    query.buildSqlAndParams();
    const [desc]: any = query.preAggregations?.preAggregationsDescription();
    expect(desc.preAggregationId).toEqual('very_long_visitors_cube_name_for_issue.monthly_amount_rollup');

    // Same naming as PreAggregationLoader (content_version, structure_version, last_updated_at).
    const structureVersion = getStructureVersion(desc);
    const ranges: [string, string][] = [
      ['2017-01-01T00:00:00.000', '2017-01-31T23:59:59.999'],
      ['2017-02-01T00:00:00.000', '2017-02-28T23:59:59.999'],
    ];
    const tables = ranges.map(range => {
      const partitionTableName = PreAggregationPartitionRangeLoader.partitionTableName(desc.tableName, desc.partitionGranularity, range);
      const targetTableName = PreAggregations.targetTableName({
        table_name: partitionTableName,
        content_version: 'abcdefgh',
        structure_version: structureVersion,
        last_updated_at: Date.UTC(2026, 0, 1),
        naming_version: 2,
      });
      const [sql, params] = dbRunner.replacePartitionName(desc.loadSql, desc, 'unused', desc.partitionGranularity, range);
      return {
        targetTableName,
        loadSql: [
          sql.replace(`${partitionTableName}_unused`, targetTableName),
          params,
        ],
      };
    });

    const relnames = tables.map(({ targetTableName }) => targetTableName.split('.')[1]);
    const res = await dbRunner.testQueries([
      ...tables.map(({ loadSql }) => loadSql),
      ['SELECT relname AS table_name FROM pg_class WHERE relnamespace = pg_my_temp_schema() AND relkind = \'r\' AND relname LIKE \'very_long%\' ORDER BY relname', []],
    ]);

    expect(res.map(r => r.table_name)).toEqual(relnames);
  });
});
