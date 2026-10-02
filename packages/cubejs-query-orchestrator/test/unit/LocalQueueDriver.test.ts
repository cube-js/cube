import type { QueryKey, QueueId } from '@cubejs-backend/base-driver';

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

  // Every caller passes the same id, like the QueryQueues sharing a state which all count from 1
  const add = async (queryKey: QueryKey) => (await connection.addToQueue(
    queryKey, 'handler', <any>['q'], 10, { queueId: 1, stageQueryKey: queryKey as string, requestId: 'r' }
  ))[1];

  const run = async (queryKey: QueryKey): Promise<QueueId> => {
    const key = connection.redisHash(queryKey);
    const queueId = await add(queryKey);
    await connection.retrieveForProcessing(key, queueId);
    await connection.setResultAndRemoveQuery(key, { result: queueId }, queueId);
    return queueId;
  };

  test('queue ids are assigned by the state, not taken from the caller', async () => {
    const first = await add('first' as QueryKey);
    const second = await add('second' as QueryKey);

    expect(first).not.toEqual(second);
    expect(await add('first' as QueryKey)).toEqual(first);
  });

  test('results expire under a single timer which stops once nothing is left to expire', async () => {
    const setIntervalSpy = jest.spyOn(global, 'setInterval');

    await run('first' as QueryKey);
    jest.advanceTimersByTime(500);
    const second = await run('second' as QueryKey);
    expect(setIntervalSpy).toHaveBeenCalledTimes(1);

    // The first tick removes only the result which has expired
    jest.advanceTimersByTime(500);
    expect([...state.resultsById.keys()]).toStrictEqual([second]);
    expect([...state.resultsByPath.keys()]).toStrictEqual([connection.redisHash('second' as QueryKey)]);
    expect(state.cleanupTimer).not.toBeNull();

    jest.advanceTimersByTime(1000);
    expect(state.resultsById.size).toBe(0);
    expect(state.resultsByPath.size).toBe(0);
    expect(state.cleanupTimer).toBeNull();
    expect(await connection.getResult('first' as QueryKey)).toBeNull();

    // A later result starts the timer again
    await run('third' as QueryKey);
    expect(setIntervalSpy).toHaveBeenCalledTimes(2);
    jest.advanceTimersByTime(1000);
    expect(state.cleanupTimer).toBeNull();
  });

  test('a pending result is kept until its run is acknowledged', async () => {
    const queryKey = 'pending' as QueryKey;
    const key = connection.redisHash(queryKey);
    const queueId = await add(queryKey);
    await connection.retrieveForProcessing(key, queueId);

    // The waiter times out, the result it created stays pending
    const waiting = connection.getResultBlocking(key, queueId);
    await run('other' as QueryKey);
    jest.advanceTimersByTime(1000);
    await expect(waiting).resolves.toBeNull();

    expect([...state.resultsById.keys()]).toStrictEqual([queueId]);
    expect(state.cleanupTimer).toBeNull();

    await connection.setResultAndRemoveQuery(key, { result: 'pending' }, queueId);
    expect(await connection.getResultBlocking(key, queueId)).toStrictEqual({ result: 'pending' });
  });

  test('a removed run drops its pending result', async () => {
    const queryKey = 'removed' as QueryKey;
    const key = connection.redisHash(queryKey);
    const queueId = await add(queryKey);
    await connection.retrieveForProcessing(key, queueId);

    const waiting = connection.getResultBlocking(key, queueId);
    await connection.getQueryAndRemove(key, queueId);
    expect(state.resultsById.size).toBe(0);

    jest.advanceTimersByTime(1000);
    await expect(waiting).resolves.toBeNull();
  });
});
