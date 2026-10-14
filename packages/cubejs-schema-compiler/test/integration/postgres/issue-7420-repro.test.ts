import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// Repro for https://github.com/cube-js/cube/issues/7420
// An `original_sql` pre-aggregation table is created as `CREATE TABLE ... AS <cube sql>`,
// so its columns carry the raw names from the cube SQL (`position_id`). Index columns,
// however, are rendered as member aliases (`base_positions__position_id`), so the
// `CREATE INDEX` statement (and the Cube Store `INDEX` clause) references a column
// that does not exist and the pre-aggregation build fails.
describe('Issue 7420: index on original_sql pre-aggregation', () => {
  jest.setTimeout(200000);

  const schemaName = 'issue_7420_pre_aggs';

  const makeCompiler = (external: boolean) => prepareYamlCompiler(`
cubes:
  - name: base_positions
    sql: >
      SELECT 'id_1'::text AS position_id, 'test'::text AS position_name
    dimensions:
      - name: position_id
        sql: position_id
        type: string
        primary_key: true
      - name: position_name
        sql: position_name
        type: string
    pre_aggregations:
      - name: main
        type: original_sql
        external: ${external}
        indexes:
          - name: position_id_index
            columns:
              - position_id
  `);

  async function describePreAgg(external: boolean): Promise<any> {
    const { compiler, joinGraph, cubeEvaluator } = makeCompiler(external);
    await compiler.compile();
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      dimensions: ['base_positions.position_id', 'base_positions.position_name'],
      timezone: 'UTC',
      preAggregationsSchema: schemaName,
    });
    const descriptions: any[] = query.preAggregations?.preAggregationsDescription() || [];
    const desc = descriptions.find((d) => d.preAggregationId === 'base_positions.main');
    expect(desc).toBeDefined();
    return desc;
  }

  it('external: false - CREATE INDEX runs against the created table in Postgres', async () => {
    const desc = await describePreAgg(false);
    const { tableName } = desc;

    const queries: [string, any[]][] = [
      [`CREATE SCHEMA IF NOT EXISTS ${schemaName}`, []],
      desc.loadSql,
      ...desc.indexesSql.map((i: any) => i.sql),
      [`SELECT indexdef FROM pg_indexes WHERE schemaname = '${schemaName}'`, []],
    ];

    try {
      const res = await dbRunner.testQueries(queries, async () => null);
      expect(res).toHaveLength(1);
      expect(res[0].indexdef).toContain('(position_id)');
    } finally {
      await dbRunner.testQueries([[`DROP SCHEMA IF EXISTS ${schemaName} CASCADE`, []]], async () => null);
    }
    expect(tableName).toContain('base_positions_main');
  });

  it('external: true - Cube Store index columns match the original_sql table columns', async () => {
    const desc = await describePreAgg(true);
    expect(desc.createTableIndexes).toHaveLength(1);
    // Columns of the uploaded table are `position_id` / `position_name` (raw cube SQL names)
    expect(desc.createTableIndexes[0].columns).toEqual(['"position_id"']);
  });
});
