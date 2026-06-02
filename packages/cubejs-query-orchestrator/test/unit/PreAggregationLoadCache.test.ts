/* eslint-disable @typescript-eslint/no-explicit-any */
import { PreAggregationLoadCache } from '../../src/orchestrator/PreAggregationLoadCache';

const FIFTEEN_DAYS_SECS = 15 * 24 * 60 * 60;
const ONE_HOUR_SECS = 60 * 60;

describe('PreAggregationLoadCache', () => {
  describe('keyQueryResult', () => {
    let cacheQueryResult: jest.Mock;
    let queryRedisKey: jest.Mock;
    let queryCache: { cacheQueryResult: jest.Mock; queryRedisKey: jest.Mock; options: Record<string, unknown> };
    let loadCache: PreAggregationLoadCache;

    const callKeyQueryResult = (renewalThreshold?: number, refreshKeyRenewalThreshold?: number) => {
      if (refreshKeyRenewalThreshold !== undefined) {
        queryCache.options.refreshKeyRenewalThreshold = refreshKeyRenewalThreshold;
      } else {
        delete queryCache.options.refreshKeyRenewalThreshold;
      }

      const sqlQuery: [string, unknown[], { renewalThreshold?: number }] = [
        'SELECT MAX(loaded_at)',
        [],
        renewalThreshold !== undefined ? { renewalThreshold } : {},
      ];

      return loadCache.keyQueryResult(sqlQuery, false, 10);
    };

    beforeEach(() => {
      cacheQueryResult = jest.fn().mockResolvedValue('result');
      queryRedisKey = jest.fn().mockReturnValue('redis-key');
      queryCache = {
        cacheQueryResult,
        queryRedisKey,
        options: {},
      };

      loadCache = new PreAggregationLoadCache(
        {} as any,
        queryCache as any,
        {} as any,
        { dataSource: 'default' },
      );
    });

    it('uses renewalThreshold for both expiration and renewal when every is long', async () => {
      await callKeyQueryResult(FIFTEEN_DAYS_SECS);

      expect(cacheQueryResult).toHaveBeenCalledWith(
        'SELECT MAX(loaded_at)',
        [],
        ['SELECT MAX(loaded_at)', []],
        FIFTEEN_DAYS_SECS,
        expect.objectContaining({
          renewalThreshold: FIFTEEN_DAYS_SECS,
        }),
      );
    });

    it('keeps minimum 1-hour expiration for short renewalThreshold (backward compatible)', async () => {
      await callKeyQueryResult(10);

      expect(cacheQueryResult).toHaveBeenCalledWith(
        'SELECT MAX(loaded_at)',
        [],
        ['SELECT MAX(loaded_at)', []],
        ONE_HOUR_SECS,
        expect.objectContaining({
          renewalThreshold: 10,
        }),
      );
    });

    it('uses refreshKeyRenewalThreshold for both expiration and renewal when set globally', async () => {
      await callKeyQueryResult(FIFTEEN_DAYS_SECS, 3600);

      expect(cacheQueryResult).toHaveBeenCalledWith(
        'SELECT MAX(loaded_at)',
        [],
        ['SELECT MAX(loaded_at)', []],
        3600,
        expect.objectContaining({
          renewalThreshold: 3600,
        }),
      );
    });
  });
});
