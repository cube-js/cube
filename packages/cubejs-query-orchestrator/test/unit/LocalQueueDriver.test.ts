import { LocalQueueDriver } from '../../src/orchestrator/LocalQueueDriver';

/**
 * Several requests coalesce onto one queue key, but a request that joins an
 * already-active key registers no interest of its own until it calls
 * `getResultBlocking`. These cover the straggler: it must still receive the
 * result the first waiter consumed, rather than being told to continue waiting.
 */
describe('LocalQueueDriver', () => {
  const driverOptions = {
    redisQueuePrefix: 'TEST_RETAINED',
    continueWaitTimeout: 1,
    heartBeatTimeout: 10,
    concurrency: 2,
    processUid: 'test-uid',
  } as any;

  const queryKey = ['SELECT 1', []] as any;

  async function enqueueAndComplete(connection: any, result: unknown) {
    const hash = connection.redisHash(queryKey);
    await connection.addToQueue(
      1,
      queryKey,
      Date.now() + 60_000,
      'query',
      { isJob: false },
      1,
      { queueId: 1, stageQueryKey: 'stage', requestId: 'req-1' },
    );
    // Mirror what a processor does: claim the lock, then publish the result.
    await connection.retrieveForProcessing(hash, 1);
    const processingId = await connection.getNextProcessingId();
    connection.state.processingLocks[hash] = processingId;
    await connection.setResultAndRemoveQuery(hash, result, processingId, 1);
    return hash;
  }

  it('serves a completed result to a waiter that arrives after the first one consumed it', async () => {
    const driver = new LocalQueueDriver({ ...driverOptions, redisQueuePrefix: 'TEST_RETAINED_A' });
    const connection: any = await driver.createConnection();

    const hash = await enqueueAndComplete(connection, 'the-result');

    expect(await connection.getResultBlocking(hash, 1)).toEqual('the-result');
    // The straggler: queryDef is gone and the promise was consumed above.
    expect(await connection.getResultBlocking(hash, 1)).toEqual('the-result');
  });

  it('does not serve a retained result once the query is cancelled', async () => {
    const driver = new LocalQueueDriver({ ...driverOptions, redisQueuePrefix: 'TEST_RETAINED_B' });
    const connection: any = await driver.createConnection();

    const hash = await enqueueAndComplete(connection, 'the-result');
    expect(await connection.getResultBlocking(hash, 1)).toEqual('the-result');

    await connection.getQueryAndRemove(hash, 1);

    expect(await connection.getResultBlocking(hash, 1)).toBeNull();
  });

  it('does not hand a retained result to the next query on the same key', async () => {
    const driver = new LocalQueueDriver({ ...driverOptions, redisQueuePrefix: 'TEST_RETAINED_C' });
    const connection: any = await driver.createConnection();

    const hash = await enqueueAndComplete(connection, 'first-result');
    expect(await connection.getResultBlocking(hash, 1)).toEqual('first-result');

    // A genuinely new execution on the same key must produce its own result,
    // not resolve instantly from what the previous one left behind.
    await enqueueAndComplete(connection, 'second-result');
    expect(await connection.getResultBlocking(hash, 1)).toEqual('second-result');
  });
});
