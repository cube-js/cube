/**
 * Repro for https://github.com/cube-js/cube/issues/7421
 *
 * A rollup with `use_original_sql_pre_aggregations: true` references the cube's
 * `original_sql` pre-aggregation by its unversioned table name
 * (`pre_aggregations.base_positions_main`). The orchestrator must rewrite it to the
 * versioned table (`..._main_<hash>`) before running the build SQL, otherwise the
 * source database fails with `relation "...base_positions_main" does not exist`.
 *
 * `refreshStoreInSourceStrategy` and the temp-table write strategy do this rewrite,
 * but the paths that run `preAggregation.sql` directly against the source (used for
 * external / Cube Store rollups on read-only drivers such as Postgres, and on drivers
 * with `unloadWithoutTempTable` such as Athena or Snowflake) do not.
 */
import { PreAggregationLoader, PreAggregationTableToTempTable } from '../../src';

class TestPreAggregationLoader extends PreAggregationLoader {
  public refreshReadOnlyExternalStrategy(client: any, newVersionEntry: any, saveCancelFn: any, invalidationKeys: any) {
    return super.refreshReadOnlyExternalStrategy(client, newVersionEntry, saveCancelFn, invalidationKeys);
  }

  public runWriteStrategy(
    client: any,
    newVersionEntry: any,
    saveCancelFn: any,
    invalidationKeys: any,
    withTempTable: boolean,
    dropSourceTempTable: boolean,
  ) {
    return super.runWriteStrategy(client, newVersionEntry, saveCancelFn, invalidationKeys, withTempTable, dropSourceTempTable);
  }

  protected async uploadExternalPreAggregation() {
    // Cube Store upload is irrelevant here
  }

  protected async cleanupWriteStrategy() {
    // No tables to drop
  }
}

const ORIGINAL_SQL_TABLE = 'pre_aggregations.base_positions_main';
const ORIGINAL_SQL_VERSIONED_TABLE = 'pre_aggregations.base_positions_main_h1xfmtvc_snxb2x1_1lbigf1';

const rollupSql = 'SELECT date_trunc(\'month\', "base_positions".date) "base_positions__position_date_month", ' +
  'count("base_positions".position_id) "base_positions__position_count" ' +
  `FROM ${ORIGINAL_SQL_TABLE} AS "base_positions" GROUP BY 1`;

const rollupPreAggregation = {
  preAggregationId: 'base_positions.positions_per_month',
  preAggregationsSchema: 'pre_aggregations',
  tableName: 'pre_aggregations.base_positions_positions_per_month',
  dataSource: 'default',
  external: true,
  sql: [rollupSql, []],
  loadSql: [`CREATE TABLE pre_aggregations.base_positions_positions_per_month AS ${rollupSql}`, []],
  invalidateKeyQueries: [],
};

// The original_sql pre-aggregation that was built first, as passed down by PreAggregations.loadAllPreAggregationsIfNeeded
const tablesToTempTables: PreAggregationTableToTempTable[] = [
  [ORIGINAL_SQL_TABLE, {
    targetTableName: ORIGINAL_SQL_VERSIONED_TABLE,
    refreshKeyValues: [],
    lastUpdatedAt: Date.now(),
  } as any],
];

const newVersionEntry = {
  table_name: 'pre_aggregations.base_positions_positions_per_month',
  content_version: 'ojdbeepf',
  structure_version: 'azqwslpx',
  last_updated_at: Date.now(),
  naming_version: 2,
};

const createLoader = () => new TestPreAggregationLoader(
  (async () => ({})) as any,
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  () => {},
  {} as any,
  { externalDriverFactory: async () => ({ capabilities: () => ({}) }) } as any,
  rollupPreAggregation,
  tablesToTempTables,
  { fetchTables: async () => [] } as any,
  { requestId: 'issue-7421' },
);

const createSourceDriver = (capabilities: Record<string, boolean> = {}) => {
  const executed: string[] = [];
  const client: any = {
    readOnly: () => true,
    capabilities: () => capabilities,
    createSchemaIfNotExists: async () => null,
    downloadQueryResults: async (sql: string) => {
      executed.push(sql);
      return { rows: [], types: [] };
    },
    query: async (sql: string) => {
      executed.push(sql);
      return [];
    },
  };
  return { client, executed };
};

const saveCancelFn: any = (p: Promise<any>) => p;

describe('issue #7421: rollup built from original_sql pre-aggregation', () => {
  test('read-only source (e.g. Postgres) + external rollup uses the versioned original_sql table', async () => {
    const { client, executed } = createSourceDriver();

    await createLoader().refreshReadOnlyExternalStrategy(client, newVersionEntry, saveCancelFn, []);

    expect(executed).toHaveLength(1);
    expect(executed[0]).toContain(`FROM ${ORIGINAL_SQL_VERSIONED_TABLE} AS "base_positions"`);
  });

  test('source without temp table (e.g. Athena, Snowflake) + external rollup uses the versioned original_sql table', async () => {
    const { client, executed } = createSourceDriver({ unloadWithoutTempTable: true });

    await createLoader().runWriteStrategy(client, newVersionEntry, saveCancelFn, [], false, false);

    expect(executed).toHaveLength(1);
    expect(executed[0]).toContain(`FROM ${ORIGINAL_SQL_VERSIONED_TABLE} AS "base_positions"`);
  });
});
