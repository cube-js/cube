import crypto from 'crypto';
import type { QueryKey, QueueDriverConnectionInterface } from '@cubejs-backend/base-driver';

import { LocalQueueDriver } from '../../src';

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
