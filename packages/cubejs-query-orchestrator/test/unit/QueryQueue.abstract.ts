import { Readable } from 'stream';
import crypto from 'crypto';

import type { QueryKey, QueueDriverInterface, QueueDriverOptions } from '@cubejs-backend/base-driver';
import { pausePromise } from '@cubejs-backend/shared';
import { CubestoreQueueDriverConnection, CubeStoreQueueDriver } from '@cubejs-backend/cubestore-driver';

import { LocalQueueDriver, QueryQueue, QueryQueueOptions } from '../../src';
import { ContinueWaitError } from '../../src/orchestrator/ContinueWaitError';
import { processUidRE } from '../../src/orchestrator/utils';

export type QueryQueueTestOptions = Pick<QueryQueueOptions, 'cacheAndQueueDriver' | 'cubeStoreDriverFactory'> & {
  beforeAll?: () => Promise<void>,
  afterAll?: () => Promise<void>,
};

class QueryQueueExtended extends QueryQueue {
  declare public queueDriver: QueueDriverInterface;

  public reconcileQueue = super.reconcileQueue;

  public processQuery = super.processQuery;

  public processCancel = super.processCancel;

  public redisHash = super.redisHash;
}

export const QueryQueueTest = (name: string, options: QueryQueueTestOptions) => {
  describe(`QueryQueue${name}`, () => {
    jest.setTimeout(10 * 1000);

    // Same switch as QueryQueue's private factoryQueueDriver, for tests that need
    // driver options the shared queue instance cannot provide
    const createQueueDriver = (driverOptions: QueueDriverOptions): QueueDriverInterface => (
      options.cacheAndQueueDriver === 'cubestore'
        ? new CubeStoreQueueDriver(() => options.cubeStoreDriverFactory!(), driverOptions)
        : new LocalQueueDriver(driverOptions)
    );

    const delayFn = (result, delay) => new Promise(resolve => setTimeout(() => resolve(result), delay));
    const logger = jest.fn((message, event) => console.log(`${message} ${JSON.stringify(event)}`));

    let delayCount = 0;
    let streamCount = 0;
    const processMessagePromises: Promise<any>[] = [];
    const processCancelPromises: Promise<any>[] = [];
    let cancelledQuery;

    const tenantPrefix = crypto.randomBytes(6).toString('hex');

    const queue = new QueryQueueExtended(`${tenantPrefix}#test_query_queue`, {
      queryHandlers: {
        foo: async (query) => `${query[0]} bar`,
        delay: async (query, setCancelHandler) => {
          const result = query.result + delayCount;
          delayCount += 1;
          await setCancelHandler(result);
          return delayFn(result, query.delay);
        },
      },
      streamHandler: async (query, stream) => {
        streamCount++;

        // TODO: Fix an issue with a fast execution of stream handler which caused by removal of QueryStream from streams,
        // while EventListener doesnt start to listen for started stream event
        await pausePromise(250);

        return new Promise((resolve, reject) => {
          const readable = Readable.from([]);
          readable.once('end', () => resolve(null));
          readable.once('close', () => resolve(null));
          readable.once('error', (err) => reject(err));
          readable.pipe(stream);
        });
      },
      sendProcessMessageFn: async (queryKeyHashed, queueId) => {
        processMessagePromises.push(queue.processQuery.bind(queue)(queryKeyHashed, queueId));
      },
      sendCancelMessageFn: async (query) => {
        processCancelPromises.push(queue.processCancel.bind(queue)(query));
      },
      cancelHandlers: {
        delay: async (query) => {
          console.log(`cancel call: ${JSON.stringify(query)}`);
          cancelledQuery = query.queryKey;
        }
      },
      continueWaitTimeout: 1,
      executionTimeout: 2,
      orphanedTimeout: 2,
      concurrency: 1,
      ...options,
      logger,
    });

    async function awaitProcessing() {
      // process query can call reconcileQueue
      while (await queue.shutdown() || processMessagePromises.length || processCancelPromises.length) {
        await Promise.all(processMessagePromises.splice(0).concat(
          processCancelPromises.splice(0)
        ));
      }
    }

    afterEach(async () => {
      await awaitProcessing();
    });

    beforeEach(() => {
      logger.mockClear();
      delayCount = 0;
      streamCount = 0;
    });

    afterAll(async () => {
      await awaitProcessing();
      // stdout conflict with console.log
      // TODO: find out why awaitProcessing doesnt work
      await pausePromise(1 * 1000);

      if (options.afterAll) {
        await options.afterAll();
      }
    });

    if (options.beforeAll) {
      beforeAll(async () => {
        await options.beforeAll();
      });
    }

    test('gutter', async () => {
      const query: QueryKey = ['select * from', []];
      const result = await queue.executeInQueue('foo', query, query);
      expect(result).toBe('select * from bar');
    });

    test('instant double wait resolve', async () => {
      const results = await Promise.all([
        queue.executeInQueue('delay', 'instant', { delay: 400, result: '2' }),
        queue.executeInQueue('delay', 'instant', { delay: 400, result: '2' })
      ]);
      expect(results).toStrictEqual(['20', '20']);
    });

    test('priority', async () => {
      const result = await Promise.all([
        queue.executeInQueue('delay', '11', { delay: 600, result: '1' }, 1),
        queue.executeInQueue('delay', '12', { delay: 100, result: '2' }, 0),
        queue.executeInQueue('delay', '13', { delay: 100, result: '3' }, 10)
      ]);
      expect(parseInt(result.find(f => f[0] === '3'), 10) % 10).toBeLessThan(2);
    });

    test('timeout - continue wait', async () => {
      const query: QueryKey = ['select * from 2', []];
      let errorString = '';

      for (let i = 0; i < 5; i++) {
        try {
          await queue.executeInQueue('delay', query, { delay: 3000, result: '1' });
          console.log(`Delay ${i}`);
        } catch (e) {
          if ((<Error>e).message === 'Continue wait') {
            // eslint-disable-next-line no-continue
            continue;
          }
          errorString = e.toString();
          break;
        }
      }

      expect(errorString).toEqual(expect.stringContaining('timeout'));
    });

    test('timeout', async () => {
      const query: QueryKey = ['select * from 3', []];

      // executionTimeout is 2s, 5s is enough
      await queue.executeInQueue('delay', query, { delay: 5 * 1000, result: '1', isJob: true });
      await awaitProcessing();

      expect(logger.mock.calls.length).toEqual(5);
      // assert that query queue is able to get query def by query key
      expect(logger.mock.calls[4][0]).toEqual('Cancelling query due to timeout');
      expect(logger.mock.calls[3][0]).toEqual('Error while querying');
    });

    test('stage reporting', async () => {
      const resultPromise = queue.executeInQueue('delay', '1', { delay: 200, result: '1' }, 0, {
        stageQueryKey: '1',
        requestId: '9f056234-aa57-4702-ab30-145221da6a46-span-1',
        spanId: 'span-id'
      });
      await delayFn(null, 50);
      expect((await queue.getQueryStage('1')).stage).toBe('Executing query');
      await resultPromise;
      expect(await queue.getQueryStage('1')).toEqual(undefined);
    });

    test('priority stage reporting', async () => {
      const resultPromise1 = queue.executeInQueue('delay', '31', { delay: 200, result: '1' }, 20, {
        stageQueryKey: '12',
        requestId: '4274691a-5f4c-480e-89c4-d2b9d989891c-span-1',
        spanId: 'span-id'
      });
      await delayFn(null, 50);
      const resultPromise2 = queue.executeInQueue('delay', '32', { delay: 200, result: '1' }, 10, {
        stageQueryKey: '12',
        requestId: '000bce99-b987-4649-ae5e-1178532929f5-span-1',
        spanId: 'span-id'
      });
      await delayFn(null, 50);

      expect((await queue.getQueryStage('12', 10)).stage).toBe('#1 in queue');
      await resultPromise1;
      await resultPromise2;
      expect(await queue.getQueryStage('12')).toEqual(undefined);
    });

    test('negative priority', async () => {
      const results = [];

      queue.executeInQueue('delay', '31', { delay: 400, result: '4' }, -10);

      await delayFn(null, 200);

      await Promise.all([
        queue.executeInQueue('delay', '32', { delay: 100, result: '3' }, -9).then(r => {
          results.push(['32', r]);
        }),
        queue.executeInQueue('delay', '33', { delay: 100, result: '2' }, -8).then(r => {
          results.push(['33', r]);
        }),
        queue.executeInQueue('delay', '34', { delay: 100, result: '1' }, -7).then(r => {
          results.push(['34', r]);
        })
      ]);

      expect(results).toEqual([
        ['34', '11'],
        ['33', '22'],
        ['32', '33'],
      ]);
    });

    test('sequence', async () => {
      const p1 = queue.executeInQueue('delay', '111', { delay: 50, result: '1' }, 0);
      const p2 = delayFn(null, 50).then(() => queue.executeInQueue('delay', '112', { delay: 50, result: '2' }, 0));
      const p3 = delayFn(null, 75).then(() => queue.executeInQueue('delay', '113', { delay: 50, result: '3' }, 0));
      const p4 = delayFn(null, 100).then(() => queue.executeInQueue('delay', '114', { delay: 50, result: '4' }, 0));

      const result = await Promise.all([p1, p2, p3, p4]);
      expect(result).toEqual(['10', '21', '32', '43']);
    });

    test('orphaned', async () => {
      // recover if previous test broken something
      for (let i = 1; i <= 4; i++) {
        await queue.executeInQueue('delay', `11${i}`, { delay: 50, result: `${i}` }, 0);
      }

      cancelledQuery = null;
      delayCount = 0;

      let result = queue.executeInQueue('delay', '111', { delay: 800, result: '1' }, 0);
      delayFn(null, 50).then(() => queue.executeInQueue('delay', '112', { delay: 800, result: '2' }, 0)).catch(e => e);
      delayFn(null, 75).then(() => queue.executeInQueue('delay', '113', { delay: 800, result: '3' }, 0)).catch(e => e);
      // orphaned timeout should be applied
      delayFn(null, 100).then(() => queue.executeInQueue('delay', '114', { delay: 900, result: '4' }, 0)).catch(e => e);

      expect(await result).toBe('10');
      await queue.executeInQueue('delay', '112', { delay: 800, result: '2' }, 0);

      result = await queue.executeInQueue('delay', '113', { delay: 900, result: '3' }, 0);
      expect(result).toBe('32');

      await delayFn(null, 500);
      expect(cancelledQuery).toBe('114');
      await queue.executeInQueue('delay', '114', { delay: 50, result: '4' }, 0);
    });

    test('orphaned with custom ttl', async () => {
      const connection = await queue.queueDriver.createConnection();

      try {
        const priority = 10;
        const time = new Date().getTime();

        expect(await connection.getOrphanedQueries()).toEqual([]);

        let orphanedTimeout = 2;
        await connection.addToQueue(['1', []], 'delay', { isJob: true, orphanedTimeout: time, }, priority, {
          queueId: 1,
          stageQueryKey: '1',
          requestId: '1',
          orphanedTimeout,
        });

        expect(await connection.getOrphanedQueries()).toEqual([]);

        orphanedTimeout = 60;

        await connection.addToQueue(['2', []], 'delay', { isJob: true, orphanedTimeout: time, }, priority, {
          queueId: 2,
          stageQueryKey: '2',
          requestId: '2',
          orphanedTimeout,
        });

        await pausePromise(2000 + 500 /*  additional timeout on CI */);

        expect(await connection.getOrphanedQueries()).toEqual([
          [
            connection.redisHash(['1', []]),
            expect.any(Number)
          ]
        ]);
      } finally {
        await connection.getQueryAndRemove(connection.redisHash(['1', []]), null);
        await connection.getQueryAndRemove(connection.redisHash(['2', []]), null);

        queue.queueDriver.release(connection);
      }
    });

    test('queue hash process persistent flag properly', () => {
      const query: QueryKey = ['select * from table', []];
      const key1 = queue.redisHash(query);
      // @ts-ignore
      query.persistent = false;
      const key2 = queue.redisHash(query);
      // @ts-ignore
      query.persistent = true;
      const key3 = queue.redisHash(query);
      const key4 = queue.redisHash(query);

      expect(key1).toEqual(key2);
      expect(key1.split('@').length).toBe(1);

      expect(key3).toEqual(key4);
      expect(key3.split('@').length).toBe(2);
      expect(processUidRE.test(key3.split('@')[1])).toBeTruthy();

      if (options.cacheAndQueueDriver === 'cubestore') {
        expect(queue.redisHash('string')).toBe('095d71cf12556b9d5e330ad575b3df5d');
      } else {
        expect(queue.redisHash('string')).toBe('string');
      }
    });

    test('stream handler', async () => {
      const key: QueryKey = ['select * from table', []];
      key.persistent = true;
      const stream = await queue.executeInQueue('stream', key, { aliasNameToMember: {} }, 0);
      await awaitProcessing();

      // QueryStream has a debounce timer to destroy stream
      // without reading it, timer will block exit for jest
      for await (const chunk of stream) {
        console.log('streaming chunk: ', chunk);
      }

      expect(streamCount).toEqual(1);
      expect(logger.mock.calls[logger.mock.calls.length - 1][0]).toEqual('Performing query completed');
    });

    test('removed before reconciled', async () => {
      const query: QueryKey = ['select * from', []];
      const key = queue.redisHash(query);
      await queue.processQuery(key, null);
      const result = await queue.executeInQueue('foo', key, query);
      expect(result).toBe('select * from bar');
    });

    // A queue item being active is the only lock there is, so these pin down the semantics both
    // drivers have to agree on. Note the suite runs with concurrency: 1.
    describe('queue driver semantics', () => {
      const priority = 10;

      const addQuery = (connection: any, queryKey: QueryKey, requestId: string) => connection.addToQueue(
        queryKey,
        'delay',
        <any>{ isJob: true },
        priority,
        { stageQueryKey: queryKey, requestId, orphanedTimeout: 60 }
      );

      const withConnections = async (count: number, fn: (...connections: any[]) => Promise<QueryKey[]>) => {
        const connections = await Promise.all(
          Array.from({ length: count }, () => queue.queueDriver.createConnection())
        );

        let keys: QueryKey[] = [];
        try {
          keys = await fn(...connections);
        } finally {
          for (const key of keys) {
            await connections[0].getQueryAndRemove(connections[0].redisHash(key), null);
          }

          connections.forEach(connection => queue.queueDriver.release(connection));
        }
      };

      test('addToQueue dedupes by key and returns the existing queue id', async () => {
        await withConnections(2, async (connection, connection2) => {
          const key: QueryKey = ['dedupe', []];

          const [added, queueId] = await addQuery(connection, key, 'dedupe-1');
          expect(added).toBe(1);

          const [addedAgain, queueIdAgain] = await addQuery(connection2, key, 'dedupe-2');
          expect(addedAgain).toBe(0);
          expect(queueIdAgain).toEqual(queueId);

          // The first add wins outright: the second one must not overwrite the payload
          const def = await connection.getQueryDef(connection.redisHash(key), queueId);
          expect(def.requestId).toBe('dedupe-1');

          return [key];
        });
      });

      test('retrieveForProcessing does not activate an already active item', async () => {
        await withConnections(2, async (connection, connection2) => {
          const key: QueryKey = ['already-active', []];
          const hash = connection.redisHash(key);

          await addQuery(connection, key, 'already-active');

          const [added, queueId, active, , def] = await connection.retrieveForProcessing(hash);
          expect(added).toBe(1);
          expect(def).toBeTruthy();
          expect(active).toContain(hash);

          const [addedAgain, queueIdAgain, activeAgain, , defAgain] = await connection2.retrieveForProcessing(hash);
          expect(addedAgain).toBe(0);
          expect(queueIdAgain).toBe(null);
          expect(defAgain).toBe(null);
          expect(activeAgain).toContain(hash);
          expect(await connection.getActiveQueries()).toEqual([[hash, queueId]]);

          return [key];
        });
      });

      test('retrieveForProcessing leaves the item pending when the queue is saturated', async () => {
        await withConnections(2, async (connection, connection2) => {
          const first: QueryKey = ['saturated-1', []];
          const second: QueryKey = ['saturated-2', []];
          const secondHash = connection.redisHash(second);

          await addQuery(connection, first, 'saturated-1');
          const [, secondQueueId] = await addQuery(connection, second, 'saturated-2');

          const [added] = await connection.retrieveForProcessing(connection.redisHash(first));
          expect(added).toBe(1);

          const [addedSecond, queueIdSecond, , , defSecond] = await connection2.retrieveForProcessing(secondHash);
          expect(addedSecond).toBe(0);
          expect(queueIdSecond).toBe(null);
          expect(defSecond).toBe(null);
          // Nothing was mutated, so it is still waiting to be picked up
          expect(await connection.getToProcessQueries()).toEqual([[secondHash, secondQueueId]]);

          return [first, second];
        });
      });

      test('retrieveForProcessing on an unknown key creates nothing', async () => {
        await withConnections(1, async (connection) => {
          const hash = connection.redisHash(['never-added', []]);

          const [added, , , , def] = await connection.retrieveForProcessing(hash);
          expect(added).toBe(0);
          expect(def).toBe(null);

          const [active, toProcess] = await connection.getQueryStageState(true);
          expect(active).not.toContain(hash);
          expect(toProcess).not.toContain(hash);
          expect(await connection.getQueryDef(hash, null)).toBe(null);

          return [];
        });
      });

      // orphanedTimeout is in seconds and the deadline is derived from it, so the only way to
      // get an orphaned item is to wait it out
      const addWithOrphanedTimeout = (connection: any, queryKey: QueryKey, requestId: string) => connection.addToQueue(
        queryKey,
        'delay',
        <any>{ isJob: true },
        priority,
        { stageQueryKey: queryKey, requestId, orphanedTimeout: 1 }
      );

      test('orphaned queries only cover pending items', async () => {
        await withConnections(1, async (connection) => {
          const key: QueryKey = ['orphaned-pending', []];
          const hash = connection.redisHash(key);

          await addWithOrphanedTimeout(connection, key, 'orphaned-pending');
          expect(await connection.getOrphanedQueries()).toEqual([]);

          await pausePromise(1000 + 500 /* additional timeout on CI */);
          expect(await connection.getOrphanedQueries()).toEqual([[hash, expect.any(Number)]]);

          const [added] = await connection.retrieveForProcessing(hash);
          expect(added).toBe(1);

          // Once it is being executed the heartbeat takes over from the orphaned deadline
          expect(await connection.getOrphanedQueries()).toEqual([]);

          return [key];
        });
      });

      test('getQueriesToCancel reports each item once', async () => {
        await withConnections(1, async (connection) => {
          const key: QueryKey = ['cancel-once', []];
          const hash = connection.redisHash(key);

          await addWithOrphanedTimeout(connection, key, 'cancel-once');
          await pausePromise(1000 + 500 /* additional timeout on CI */);

          const toCancel = await connection.getQueriesToCancel();
          expect(toCancel.filter(([queryKey]) => queryKey === hash)).toHaveLength(1);

          return [key];
        });
      });

      test('setResultAndRemoveQuery reports failure for a removed item', async () => {
        await withConnections(1, async (connection) => {
          const key: QueryKey = ['ack-removed', []];
          const hash = connection.redisHash(key);

          const [, queueId] = await addQuery(connection, key, 'ack-removed');
          await connection.retrieveForProcessing(hash);

          // Emulates the query being cancelled or orphaned while it was executing
          const [removed] = await connection.getQueryAndRemove(hash, queueId);
          expect(removed).toBeTruthy();

          expect(await connection.setResultAndRemoveQuery(hash, { result: 'late' }, queueId)).toBe(false);

          return [];
        });
      });

      test('stalled queries only cover active items with an old heartbeat', async () => {
        // heartBeatTimeout is derived from heartBeatInterval * 4, which is way too long here,
        // so this needs its own driver instance
        const driver = createQueueDriver({
          redisQueuePrefix: `${crypto.randomBytes(6).toString('hex')}#stalled`,
          concurrency: 1,
          continueWaitTimeout: 1,
          orphanedTimeout: 60,
          heartBeatTimeout: 1,
        });

        const connection: any = await driver.createConnection();
        const key: QueryKey = ['stalled', []];
        const hash = connection.redisHash(key);

        try {
          const [, queueId] = await addQuery(connection, key, 'stalled');

          // Pending items are never stalled, no matter how long they sit there
          await pausePromise(1500);
          expect(await connection.getStalledQueries()).toEqual([]);

          const [added] = await connection.retrieveForProcessing(hash);
          expect(added).toBe(1);
          expect(await connection.getStalledQueries()).toEqual([]);

          await pausePromise(1500);
          expect(await connection.getStalledQueries()).toEqual([[hash, queueId]]);

          await connection.updateHeartBeat(hash, queueId);
          expect(await connection.getStalledQueries()).toEqual([]);
        } finally {
          await connection.getQueryAndRemove(hash, null);
          driver.release(connection);
        }
      });
    });

    // eslint-disable-next-line no-unused-expressions
    options.cacheAndQueueDriver === 'cubestore' && describe('with CUBEJS_QUEUE_EXTERNAL_ID enabled', () => {
      jest.setTimeout(10 * 1000);

      beforeAll(() => {
        process.env.CUBEJS_QUEUE_EXTERNAL_ID = 'true';
      });

      afterAll(() => {
        delete process.env.CUBEJS_QUEUE_EXTERNAL_ID;
      });

      test('useExternalId should return true', async () => {
        const connection = await queue.queueDriver.createConnection();
        try {
          expect(await (connection as CubestoreQueueDriverConnection).useExternalId()).toBe(true);
        } finally {
          queue.queueDriver.release(connection);
        }
      });

      test('no-cache queries should not loop with concurrent clients', async () => {
        const query: QueryKey = ['select * from no_cache_test', []];

        // Two clients execute the same query concurrently with different requestIds.
        // delay=1500ms > continueWaitTimeout=1s, so both will get ContinueWaitError.
        const clientA = queue
          .executeInQueue('delay', query, { delay: 1500, result: '1' }, 0, {
            stageQueryKey: query, requestId: '70b0b0a6-60ff-43ee-95ca-b5a3d864879f-span-1', spanId: 'span-A'
          })
          .catch(e => e);
        const clientB = queue
          .executeInQueue('delay', query, { delay: 1500, result: '1' }, 0, {
            stageQueryKey: query, requestId: '8030e1f2-5e14-4241-9481-46e34d478131-span-1', spanId: 'span-B'
          })
          .catch(e => e);

        const [errA, errB] = await Promise.all([clientA, clientB]);
        expect(errA).toBeInstanceOf(ContinueWaitError);
        expect(errB).toBeInstanceOf(ContinueWaitError);

        await awaitProcessing();

        // Both clients retry (with new span suffix, same UUID prefix).
        // Both should find the existing result without triggering re-execution.
        const [resultA, resultB] = await Promise.all([
          queue.executeInQueue('delay', query, { delay: 1500, result: '1' }, 0, {
            stageQueryKey: query, requestId: '70b0b0a6-60ff-43ee-95ca-b5a3d864879f-span-2', spanId: 'span-A2'
          }),
          queue.executeInQueue('delay', query, { delay: 1500, result: '1' }, 0, {
            stageQueryKey: query, requestId: '8030e1f2-5e14-4241-9481-46e34d478131-span-2', spanId: 'span-B2'
          }),
        ]);

        expect(resultA).toBeDefined();
        expect(resultB).toBeDefined();

        // The query handler should have been called exactly once, not re-queued on retry
        expect(delayCount).toBe(1);
      });

      test('single client long polling loop should not re-execute query', async () => {
        jest.setTimeout(30 * 1000);

        const query: QueryKey = ['select * from long_poll_loop_test', []];
        const requestUuid = 'a1b2c3d4-e5f6-7890-abcd-ef1234567890';
        let spanCounter = 1;

        // Emulate query orchestrator long polling loop:
        // client keeps calling executeInQueue with the same requestId UUID prefix
        // and incrementing span suffix, just like the real orchestrator does on
        // ContinueWaitError retries. No manual awaitProcessing — query executes
        // naturally in the background while the client retries.
        let result: any = null;
        const deadline = Date.now() + 10000;

        while (Date.now() < deadline) {
          try {
            result = await queue.executeInQueue('delay', query, { delay: 1500, result: '1' }, 0, {
              stageQueryKey: query,
              requestId: `${requestUuid}-span-${spanCounter++}`,
              spanId: `span-${spanCounter}`,
            });
            break;
          } catch (e) {
            if (e instanceof ContinueWaitError) {
              // eslint-disable-next-line no-continue
              continue;
            }
            throw e;
          }
        }

        expect(result).toBeDefined();
        // The query handler should have been called exactly once, not re-queued on retry
        expect(delayCount).toBe(1);

        // CubeStore supports read-many via external_id, so the result should
        // still be available. Local driver consumes the result on first read.
        if (options.cacheAndQueueDriver === 'cubestore') {
          const secondResult = await queue.executeInQueue('delay', query, { delay: 1500, result: '1' }, 0, {
            stageQueryKey: query,
            requestId: `${requestUuid}-span-${spanCounter++}`,
            spanId: `span-${spanCounter}`,
          });
          expect(secondResult).toBeDefined();
          expect(delayCount).toBe(1);
        }
      }, 30000);
    });
  });
};
