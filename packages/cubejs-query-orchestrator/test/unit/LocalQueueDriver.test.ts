import { LocalQueueDriver } from '../../src/orchestrator/LocalQueueDriver';

describe('LocalQueueDriver', () => {
  afterEach(() => {
    jest.useRealTimers();
  });

  // https://github.com/cube-js/cube/issues/4198
  it('getResultBlocking does not leave a pending timer once the result arrives', async () => {
    jest.useFakeTimers();

    const driver = new LocalQueueDriver({
      redisQueuePrefix: 'issue-4198',
      concurrency: 1,
      continueWaitTimeout: 30,
      orphanedTimeout: 120,
      heartBeatTimeout: 30,
    });
    const connection: any = await driver.createConnection();

    const queryKeyHash = connection.redisHash(['SELECT 1', []]);
    const resultPromise = connection.getResultPromise(connection.resultListKey(queryKeyHash));
    resultPromise.resolve({ data: 'ok' });

    await expect(connection.getResultBlocking(queryKeyHash)).resolves.toEqual({ data: 'ok' });

    // The 30s continueWaitTimeout timer must be cleared, otherwise it keeps the
    // process alive after shutdown until it fires.
    expect(jest.getTimerCount()).toBe(0);
  });
});
