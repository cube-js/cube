import crypto from 'crypto';
import type { QueryKey } from '@cubejs-backend/base-driver';

import { LocalQueueDriver } from '../../src';

// https://github.com/cube-js/cube/issues/4198
// getResultBlocking races the result promise against a setTimeout that is never
// cleared, so the timer keeps the process alive for continueWaitTimeout seconds
// after the result already arrived (e.g. on shutdown).
describe('issue #4198: LocalQueueDriver leaks continueWaitTimeout timer', () => {
  afterEach(() => {
    jest.useRealTimers();
  });

  test('getResultBlocking clears its wait timer once the result is delivered', async () => {
    jest.useFakeTimers();

    const driver = new LocalQueueDriver({
      redisQueuePrefix: `${crypto.randomBytes(6).toString('hex')}#issue_4198`,
      concurrency: 1,
      continueWaitTimeout: 60,
      orphanedTimeout: 600,
      heartBeatTimeout: 600,
    });
    const connection = await driver.createConnection();

    const query: QueryKey = ['select 4198', []];
    const [, queueId] = await connection.addToQueue(
      query,
      'delay',
      { isJob: true },
      10,
      { queueId: 4198, stageQueryKey: query, requestId: 'issue-4198', orphanedTimeout: 600 }
    );
    const hash = connection.redisHash(query);
    expect(await connection.retrieveForProcessing(hash, queueId)).not.toBeNull();

    const timersBefore = jest.getTimerCount();

    const resultPromise = connection.getResultBlocking(hash, queueId);
    expect(await connection.setResultAndRemoveQuery(hash, { data: 'ok' }, queueId)).toBe(true);
    expect(await resultPromise).toEqual({ data: 'ok' });

    // No timer from getResultBlocking should outlive the call
    expect(jest.getTimerCount()).toBe(timersBefore);
  });
});
