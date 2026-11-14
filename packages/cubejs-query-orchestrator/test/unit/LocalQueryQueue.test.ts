import crypto from 'crypto';
import type { QueryKey, QueueDriverConnectionInterface } from '@cubejs-backend/base-driver';

import { LocalQueueDriver, LocalQueueDriverConnection, LocalQueueDriverConnectionState } from '../../src';

describe('LocalQueueDriver', () => {
  const priority = 10;
  let queueIdCounter = 1;

  // Driver state is shared per prefix, so a random one gives every test its own queue
  const createDriver = (concurrency = 1) => new LocalQueueDriver({
    redisQueuePrefix: `${crypto.randomBytes(6).toString('hex')}#local_query_queue`,
    concurrency,
    continueWaitTimeout: 1,
    orphanedTimeout: 60,
    heartBeatTimeout: 60,
  });

  const addQuery = (connection: QueueDriverConnectionInterface, queryKey: QueryKey, requestId: string, orphanedTimeout = 60) => connection.addToQueue(
    queryKey,
    'delay',
    { isJob: true },
    priority,
    { queueId: queueIdCounter++, stageQueryKey: queryKey, requestId, orphanedTimeout }
  );

  test('addToQueue never retrieves in memory', async () => {
    const connection = await createDriver().createConnection();
    const query: QueryKey = ['select * from add_and_retrieve', []];

    const [added, , , , retrieved] = await addQuery(connection, query, '1');

    expect(added).toBe(1);
    expect(retrieved).toBeNull();
    expect(await connection.getToProcessQueries()).toStrictEqual([
      [connection.redisHash(query), expect.any(Number)]
    ]);
  });

  // With concurrency: 1 the second retrieval is rejected by the full slot alone, so this
  // needs a free slot to show that the status is what rejects it
  test('retrieveForProcessing does not activate an already active item with a free slot', async () => {
    const driver = createDriver(2);
    const connection = await driver.createConnection();
    const connection2 = await driver.createConnection();
    const key: QueryKey = ['already-active-free-slot', []];
    const hash = connection.redisHash(key);

    const [, queueId] = await addQuery(connection, key, 'already-active-free-slot');

    expect(await connection.retrieveForProcessing(hash, queueId)).toMatchObject({
      active: [hash],
      queueSize: 0,
      def: { queryKey: key },
    });
    expect(await connection2.retrieveForProcessing(hash, queueId)).toBeNull();
    expect(await connection.getActiveQueries()).toEqual([[hash, queueId]]);
  });

  describe('result of a coalesced query', () => {
    const complete = async (connection: QueueDriverConnectionInterface, key: QueryKey, result: unknown) => {
      const hash = connection.redisHash(key);
      const [, queueId] = await addQuery(connection, key, 'first');
      await connection.retrieveForProcessing(hash, queueId);
      // A second request coalesces onto the queued key before it completes
      await addQuery(connection, key, 'straggler');
      await connection.setResultAndRemoveQuery(hash, result, queueId);

      return { hash, queueId };
    };

    test('a waiter arriving after the first consumed the result still gets it', async () => {
      const connection = await createDriver().createConnection();
      const key: QueryKey = ['coalesced-straggler', []];

      const { hash, queueId } = await complete(connection, key, { rows: [1] });

      expect(await connection.getResultBlocking(hash, queueId)).toEqual({ rows: [1] });
      expect(await connection.getResultBlocking(hash, queueId)).toEqual({ rows: [1] });
    });

    test('a later query on the same key waits for its own result', async () => {
      const connection = await createDriver().createConnection();
      const key: QueryKey = ['coalesced-next-query', []];

      const { hash, queueId } = await complete(connection, key, { rows: [1] });
      expect(await connection.getResultBlocking(hash, queueId)).toEqual({ rows: [1] });

      const [, nextQueueId] = await addQuery(connection, key, 'next');
      const pending = connection.getResultBlocking(hash, nextQueueId);
      await connection.retrieveForProcessing(hash, nextQueueId);
      await connection.setResultAndRemoveQuery(hash, { rows: [2] }, nextQueueId);

      expect(await pending).toEqual({ rows: [2] });
    });

    test('the retained result expires', async () => {
      const connection = await createDriver().createConnection();
      const key: QueryKey = ['coalesced-expired', []];

      const { hash, queueId } = await complete(connection, key, { rows: [1] });
      expect(await connection.getResultBlocking(hash, queueId)).toEqual({ rows: [1] });

      const now = Date.now();
      jest.spyOn(Date, 'now').mockReturnValue(now + 6000);
      try {
        expect(await connection.getResultBlocking(hash, queueId)).toBeNull();
      } finally {
        jest.restoreAllMocks();
      }
    });

    test('only the most recent 100 results are retained', async () => {
      const connection = await createDriver().createConnection();
      const completed: Awaited<ReturnType<typeof complete>>[] = [];

      for (let i = 0; i <= 100; i++) {
        const key: QueryKey = [`coalesced-cap-${i}`, []];
        const done = await complete(connection, key, { rows: [i] });
        expect(await connection.getResultBlocking(done.hash, done.queueId)).toEqual({ rows: [i] });
        completed.push(done);
      }

      const [first] = completed;
      const last = completed[completed.length - 1];
      expect(await connection.getResultBlocking(first.hash, first.queueId)).toBeNull();
      expect(await connection.getResultBlocking(last.hash, last.queueId)).toEqual({ rows: [100] });
    });

    test('retained results are released without further traffic', async () => {
      jest.useFakeTimers();
      try {
        const options = {
          redisQueuePrefix: 'coalesced-idle',
          concurrency: 1,
          continueWaitTimeout: 1,
          orphanedTimeout: 60,
          heartBeatTimeout: 60,
        };
        const state = new LocalQueueDriverConnectionState();
        const connection = new LocalQueueDriverConnection(new LocalQueueDriver(options), state, options);
        const key: QueryKey = ['coalesced-idle', []];

        const { hash, queueId } = await complete(connection, key, { rows: [1] });
        expect(await connection.getResultBlocking(hash, queueId)).toEqual({ rows: [1] });
        expect(state.retainedResults.size).toBe(1);

        jest.advanceTimersByTime(5001);
        expect(state.retainedResults.size).toBe(0);
      } finally {
        jest.useRealTimers();
      }
    });

    test('a cancelled query drops the retained result', async () => {
      const connection = await createDriver().createConnection();
      const key: QueryKey = ['coalesced-cancelled', []];

      const { hash, queueId } = await complete(connection, key, { rows: [1] });
      expect(await connection.getResultBlocking(hash, queueId)).toEqual({ rows: [1] });

      const [, cancelledQueueId] = await addQuery(connection, key, 'cancelled');
      await connection.cancelQuery(key, cancelledQueueId);

      expect(await connection.getResultBlocking(hash, queueId)).toBeNull();
    });

    test('a probe for a finished query does not see the retained result', async () => {
      const connection = await createDriver().createConnection();
      const key: QueryKey = ['coalesced-probe', []];

      const { hash, queueId } = await complete(connection, key, { rows: [1] });
      expect(await connection.getResultBlocking(hash, queueId)).toEqual({ rows: [1] });

      expect(await connection.getResult(key)).toBeNull();
    });
  });

  // Cube Store keeps the deadline of the first add
  describe('orphaned deadline on re-add', () => {
    const start = 97_800_000;

    beforeEach(() => jest.useFakeTimers({ now: start }));
    afterEach(() => jest.useRealTimers());

    const setTime = (ms: number) => jest.setSystemTime(start + ms);

    test('re-adding a pending query extends its orphaned deadline', async () => {
      const connection = await createDriver().createConnection();
      const key: QueryKey = ['orphaned-extended', []];
      const hash = connection.redisHash(key);

      await addQuery(connection, key, 'orphaned-extended-1', 1);

      setTime(700);
      await addQuery(connection, key, 'orphaned-extended-2', 1);

      setTime(1500);
      expect(await connection.getOrphanedQueries()).toEqual([]);

      setTime(1800);
      expect(await connection.getOrphanedQueries()).toEqual([[hash, expect.any(Number)]]);
    });

    test('re-adding with a shorter orphaned timeout keeps the later deadline', async () => {
      const connection = await createDriver().createConnection();
      const key: QueryKey = ['orphaned-kept', []];
      const hash = connection.redisHash(key);

      await addQuery(connection, key, 'orphaned-kept-1', 60);
      await addQuery(connection, key, 'orphaned-kept-2', 1);

      setTime(1500);
      expect(await connection.getOrphanedQueries()).toEqual([]);

      setTime(60500);
      expect(await connection.getOrphanedQueries()).toEqual([[hash, expect.any(Number)]]);
    });
  });
});
