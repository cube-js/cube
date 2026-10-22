// Repro for https://github.com/cube-js/cube/issues/4198
// "Leaking Timeout In LocalQueueDriver": getResultBlocking() races the result promise against a
// setTimeout(continueWaitTimeout) that is never cleared once the result arrives, so the timer keeps
// the Node.js process alive for up to continueWaitTimeout seconds after shutdown.
import { LocalQueueDriver } from '../../src/orchestrator/LocalQueueDriver';

describe('issue #4198: LocalQueueDriver getResultBlocking leaks continueWaitTimeout timer', () => {
  afterEach(() => {
    jest.useRealTimers();
  });

  it('does not leave a pending timer after the result has been delivered', async () => {
    jest.useFakeTimers();

    const driver = new LocalQueueDriver({
      redisQueuePrefix: `issue-4198-${Date.now()}`,
      concurrency: 1,
      continueWaitTimeout: 60,
      orphanedTimeout: 120,
      heartBeatTimeout: 30,
      getQueueEventsBus: undefined,
    } as any);

    const conn = await driver.createConnection();
    const queryKey: any = ['SELECT 1', []];
    const queryKeyHash = conn.redisHash(queryKey);
    const queueId = 1;

    await conn.addToQueue(queryKey, 'query', { query: 'SELECT 1' } as any, 0, { queueId, requestId: 'r1' });
    await conn.retrieveForProcessing(queryKeyHash, queueId);

    expect(jest.getTimerCount()).toBe(0);

    const resultPromise = conn.getResultBlocking(queryKeyHash, queueId);
    await conn.setResultAndRemoveQuery(queryKeyHash, { result: 'ok' }, queueId);

    await expect(resultPromise).resolves.toEqual({ result: 'ok' });

    // The result has been delivered; nothing is awaiting the continueWaitTimeout any more,
    // so no timer should remain scheduled (it would keep the process alive for 60s).
    expect(jest.getTimerCount()).toBe(0);
  });
});
