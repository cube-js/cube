// https://github.com/cube-js/cube/issues/9638
// A rollup_lambda with union_with_source_data in a non-UTC timezone reads the
// source rows past the last built partition through the lambda query, bounded by
// `afterDate FROM_PARTITION_RANGE`. Partition SQL gets FROM/TO_PARTITION_RANGE
// converted from the pre-aggregation's local time to UTC (inDbTimeZone), but the
// lambda query receives the local `buildRangeEnd` unconverted. In Europe/Madrid
// (UTC+2 in summer) the lambda lower bound is 2 hours too late and the rows
// between the partition end and that bound are in neither the partition nor the
// lambda table.
import { FROM_PARTITION_RANGE } from '@cubejs-backend/shared';

import { PreAggregationLoader, PreAggregationPartitionRangeLoader } from '../../src';

describe('Issue #9638 - rollup lambda source query bound with a timezone', () => {
  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('passes the partition end to the lambda query in UTC', async () => {
    // Local (Europe/Madrid, CEST = UTC+2) end of the built range.
    const buildRangeEnd = '2025-05-31T01:59:59.000';
    const renewQuery = jest.fn().mockResolvedValue({ data: { rowCount: 0, types: [], csvRows: '' } });

    const loader = new PreAggregationPartitionRangeLoader(
      {} as any, // driverFactory
      // eslint-disable-next-line @typescript-eslint/no-empty-function
      () => {}, // logger
      { options: {}, renewQuery } as any, // queryCache
      {} as any, // preAggregations
      {
        tableName: 'stb_pre_aggregations.test_main',
        preAggregationId: 'Test.main',
        rollupLambdaId: 'Test.lambda',
        lastRollupLambda: true,
        unionWithSourceData: true,
        partitionGranularity: 'month',
        timezone: 'Europe/Madrid',
        timestampFormat: 'YYYY-MM-DDTHH:mm:ss.SSS',
        timestampPrecision: 3,
        dataSource: 'default',
        partitionInvalidateKeyQueries: [],
        preAggregationStartEndQueries: [['SELECT MIN(ts)', [], {}], ['SELECT MAX(ts)', [], {}]],
        loadSql: ['CREATE TABLE stb_pre_aggregations.test_main AS SELECT 1', []],
        sql: ['SELECT 1', []],
      } as any,
      [], // preAggregationsTablesToTempTables
      { getTableColumnTypes: jest.fn().mockResolvedValue([{ name: 'ts', type: 'timestamp' }]) } as any,
      {
        lambdaQuery: {
          sqlAndParams: ['SELECT * FROM test WHERE "createdAt" > $1::timestamptz', [FROM_PARTITION_RANGE]],
          cacheKeyQueries: [],
        },
      } as any,
    );

    jest.spyOn(loader as any, 'partitionRanges').mockResolvedValue({
      buildRange: ['2025-05-01T00:00:00.000', buildRangeEnd],
      partitionRanges: [['2025-05-01T00:00:00.000', buildRangeEnd]],
    });
    jest.spyOn(PreAggregationLoader.prototype, 'loadPreAggregation').mockResolvedValue({
      targetTableName: 'stb_pre_aggregations.test_main20250501_abc_def',
      refreshKeyValues: [],
      lastUpdatedAt: 1,
      buildRangeEnd,
    } as any);

    await loader.loadPreAggregations();

    expect(renewQuery).toHaveBeenCalledTimes(1);
    const values = renewQuery.mock.calls[0][1];
    // The partition was loaded with `<= 2025-05-30T23:59:59.000` UTC, so the lambda
    // query must continue right after that UTC instant.
    expect(values).toEqual([PreAggregationPartitionRangeLoader.inDbTimeZone(
      { timezone: 'Europe/Madrid', timestampFormat: 'YYYY-MM-DDTHH:mm:ss.SSS' },
      buildRangeEnd,
    )]);
    expect(values[0]).toMatch(/^2025-05-30T23:59:59\.000/);
  });
});
