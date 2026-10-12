/* eslint-disable @typescript-eslint/no-explicit-any */
import { PreAggregationLoader } from '../../src';

// https://github.com/cube-js/cube/issues/7421
// A rollup with `use_original_sql_pre_aggregations: true` stored externally (Cube Store)
// and built from a read-only source (Postgres driver is read-only by default) sends its
// SQL to the source without rewriting the un-versioned `original_sql` table name
// (`stb_pre_aggregations.base_positions_main`) to the actual versioned table.

class TestLoader extends PreAggregationLoader {
  public refreshReadOnlyExternalStrategy(client: any, newVersionEntry: any, saveCancelFn: any, invalidationKeys: any) {
    return super.refreshReadOnlyExternalStrategy(client, newVersionEntry, saveCancelFn, invalidationKeys);
  }
}

describe('Issue #7421: rollup over original_sql pre-aggregation with read-only source', () => {
  const originalSqlTable = 'stb_pre_aggregations.base_positions_main';
  const versionedOriginalSqlTable = 'stb_pre_aggregations.base_positions_main_abcd1234_efgh5678_1hxyz';
  const rollupSql = `SELECT date_trunc('month', ("base_positions".date::timestamptz AT TIME ZONE 'UTC')) "base_positions__position_date_month", count("base_positions".position_id) "base_positions__position_count" \nFROM  ${originalSqlTable}  AS "base_positions"\nGROUP BY 1`;

  it('uses the versioned original_sql table name in the query sent to the source', async () => {
    const executedSql: string[] = [];
    const sourceDriver: any = {
      readOnly: () => true,
      downloadQueryResults: async (sql: string) => {
        executedSql.push(sql);
        return { rows: [], types: [] };
      },
    };
    const externalDriver: any = {
      capabilities: () => ({}),
    };

    const loader = new TestLoader(
      async () => sourceDriver,
      // eslint-disable-next-line @typescript-eslint/no-empty-function
      () => {},
      { options: {} } as any,
      { externalDriverFactory: async () => externalDriver } as any,
      {
        preAggregationId: 'base_positions.positions_per_month',
        tableName: 'stb_pre_aggregations.base_positions_positions_per_month',
        external: true,
        dataSource: 'default',
        sql: [rollupSql, []],
        loadSql: [`CREATE TABLE stb_pre_aggregations.base_positions_positions_per_month AS ${rollupSql}`, []],
        invalidateKeyQueries: [],
      },
      [[originalSqlTable, { targetTableName: versionedOriginalSqlTable } as any]],
      { fetchTables: async () => [] } as any,
      { requestId: 'issue-7421' },
    );
    jest.spyOn(loader as any, 'uploadExternalPreAggregation').mockResolvedValue(undefined);

    await loader.refreshReadOnlyExternalStrategy(
      sourceDriver,
      {
        table_name: 'stb_pre_aggregations.base_positions_positions_per_month',
        content_version: 'c',
        structure_version: 's',
        last_updated_at: 1,
        naming_version: 2,
      },
      (p: any) => p,
      [],
    );

    expect(executedSql).toHaveLength(1);
    expect(executedSql[0]).toContain(versionedOriginalSqlTable);
    expect(executedSql[0]).not.toMatch(/stb_pre_aggregations\.base_positions_main\s/);
  });
});
