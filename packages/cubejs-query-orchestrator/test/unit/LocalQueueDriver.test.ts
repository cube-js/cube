import type { QueryKey } from '@cubejs-backend/base-driver';

import { LocalQueueDriver } from '../../src/orchestrator/LocalQueueDriver';
import { LocalQueueDriverConnection, LocalQueueDriverConnectionState } from '../../src/orchestrator/LocalQueueDriverConnection';

describe('LocalQueueDriverConnection', () => {
  const options = {
    redisQueuePrefix: 'local_queue_driver_test',
    concurrency: 2,
    continueWaitTimeout: 1,
    orphanedTimeout: 120,
    heartBeatTimeout: 30,
  };

  let state: LocalQueueDriverConnectionState;
  let connection: LocalQueueDriverConnection;

  beforeEach(() => {
    jest.useFakeTimers();
    state = new LocalQueueDriverConnectionState();
    connection = new LocalQueueDriverConnection(new LocalQueueDriver(options), state, options);
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  const run = async (queryKey: QueryKey, queueId: number) => {
    const key = connection.redisHash(queryKey);
    await connection.addToQueue(queryKey, 'handler', <any>['q'], 10, { queueId, stageQueryKey: key, requestId: 'r' });
    await connection.retrieveForProcessing(key, queueId);
    await connection.setResultAndRemoveQuery(key, { result: queueId }, queueId);
    return key;
  };

  test('results expire under a single timer which stops once nothing is left to expire', async () => {
    const setIntervalSpy = jest.spyOn(global, 'setInterval');

    await run('first' as QueryKey, 1);
    jest.advanceTimersByTime(500);
    const second = await run('second' as QueryKey, 2);
    expect(setIntervalSpy).toHaveBeenCalledTimes(1);

    // The first tick removes only the result which has expired
    jest.advanceTimersByTime(500);
    expect(Object.keys(state.results)).toStrictEqual([`${connection.resultListKey(second)}_2`]);
    expect(state.lastResultQueueId).toStrictEqual({ [second]: 2 });
    expect(state.cleanupTimer).not.toBeNull();

    jest.advanceTimersByTime(1000);
    expect(state.results).toStrictEqual({});
    expect(state.lastResultQueueId).toStrictEqual({});
    expect(state.cleanupTimer).toBeNull();
    expect(await connection.getResult('first' as QueryKey)).toBeNull();

    // A later result starts the timer again
    await run('third' as QueryKey, 3);
    expect(setIntervalSpy).toHaveBeenCalledTimes(2);
    jest.advanceTimersByTime(1000);
    expect(state.cleanupTimer).toBeNull();
  });

  test('a pending result is kept until its run is acknowledged', async () => {
    const queryKey = 'pending' as QueryKey;
    const key = connection.redisHash(queryKey);
    await connection.addToQueue(queryKey, 'handler', <any>['q'], 10, { queueId: 1, stageQueryKey: key, requestId: 'r' });
    await connection.retrieveForProcessing(key, 1);

    // The waiter times out, the result it created stays pending
    const waiting = connection.getResultBlocking(key, 1);
    await run('other' as QueryKey, 2);
    jest.advanceTimersByTime(1000);
    await expect(waiting).resolves.toBeNull();

    expect(Object.keys(state.results)).toStrictEqual([`${connection.resultListKey(key)}_1`]);
    expect(state.cleanupTimer).toBeNull();

    await connection.setResultAndRemoveQuery(key, { result: 'pending' }, 1);
    expect(await connection.getResultBlocking(key, 1)).toStrictEqual({ result: 'pending' });
  });
});
