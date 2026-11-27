import { FROM_PARTITION_RANGE, MAX_SOURCE_ROW_LIMIT } from '@cubejs-backend/shared';
import type { TableStructure } from '@cubejs-backend/base-driver';

import {
  PreAggregationLoadCache,
  PreAggregationPartitionRangeLoader,
  PreAggregations,
  QueryCache,
  QueryCacheOptions,
  type PreAggregationDescription,
  type PreAggsPartitionRangeLoaderOpts,
} from '../../src';
import { partialMock, unusedDriverFactory } from './mocks';

class TestPartitionRangeLoader extends PreAggregationPartitionRangeLoader {
  public downloadLambdaTable(fromDate: string, lambdaTypes: TableStructure) {
    return super.downloadLambdaTable(fromDate, lambdaTypes);
  }
}

// @link https://github.com/cube-js/cube/issues/9638
describe('lambda source query in a non-UTC time zone', () => {
  // `buildRangeEnd` of the last partition is a local timestamp, like the partition bounds.
  const buildRangeEnd = '2025-05-31T01:59:59.000';

  test.each([
    ['Europe/Madrid', 'YYYY-MM-DD[T]HH:mm:ss.SSS[Z]'],
    ['America/New_York', 'YYYY-MM-DDTHH:mm:ss.SSS'],
  ])('passes the build range end to the source query in UTC (%s)', async (timezone, timestampFormat) => {
    const renewQuery = jest.fn().mockResolvedValue({ data: { rowCount: 0, types: [], csvRows: '' } });
    const preAggregation = {
      preAggregationId: 'Test.main',
      tableName: 'dev_pre_aggregations.test_main',
      timezone,
      timestampFormat,
      timestampPrecision: 3,
      dataSource: 'default',
      rollupLambdaId: 'Test.lambda',
      lastRollupLambda: true,
      unionWithSourceData: true,
    } as PreAggregationDescription;

    const loader = new TestPartitionRangeLoader(
      unusedDriverFactory,
      () => undefined,
      partialMock<QueryCache>({ options: {} as QueryCacheOptions, renewQuery }),
      partialMock<PreAggregations>({}),
      preAggregation,
      [],
      partialMock<PreAggregationLoadCache>({}),
      partialMock<PreAggsPartitionRangeLoaderOpts>({
        maxSourceRowLimit: 200000,
        lambdaQuery: {
          // Same shape as the generated lambda query: `"createdAt" > $1::timestamptz`
          sqlAndParams: ['SELECT * FROM test WHERE "createdAt" > $1::timestamptz LIMIT $2', [FROM_PARTITION_RANGE, MAX_SOURCE_ROW_LIMIT]],
          cacheKeyQueries: [],
        },
      }),
    );

    await loader.downloadLambdaTable(buildRangeEnd, []);

    // Partition load SQL gets its FROM/TO_PARTITION_RANGE params converted with inDbTimeZone,
    // so the lambda lower bound has to be in the same (UTC) time zone, or rows between the
    // batch layer's end and the lambda start are lost.
    const [, values] = renewQuery.mock.calls[0];
    expect(values).toEqual([
      PreAggregationPartitionRangeLoader.inDbTimeZone(preAggregation, buildRangeEnd),
      200000,
    ]);
  });
});
