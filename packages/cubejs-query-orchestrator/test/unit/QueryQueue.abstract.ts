import { Readable } from 'stream';
import crypto from 'crypto';

import type { QueryKey, QueueDriverInterface } from '@cubejs-backend/base-driver';
import { QueuePriority } from '@cubejs-backend/base-driver';
import { pausePromise } from '@cubejs-backend/shared';
import { CubeStoreDriver, CubestoreQueueDriverConnection } from '@cubejs-backend/cubestore-driver';

import { QueryQueue, QueryQueueOptions } from '../../src';
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

    const delayFn = (result, delay) => new Promise(resolve => setTimeout(() => resolve(result), delay));
    const logger = jest.fn((message, event) => console.log(`${message} ${JSON.stringify(event)}`));

    let delayCount = 0;
    let streamCount = 0;
    // A cushion which keeps the order of the log calls deterministic for the tests which
    // assert on it, a handler without it completes while executeInQueue is still logging
    let streamHandlerDelay = 250;
    const processMessagePromises: Promise<any>[] = [];
    const processCancelPromises: Promise<any>[] = [];
    let cancelledQuery;
    let streamCallOrder: string[] = [];

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

        if (streamHandlerDelay) {
          await pausePromise(streamHandlerDelay);
        }

        return new Promise((resolve, reject) => {
          const readable = Readable.from([]);
          readable.once('end', () => resolve(null));
          readable.once('close', () => resolve(null));
          readable.once('error', (err) => reject(err));
          readable.pipe(stream);
        });
      },
      sendProcessMessageFn: async (retrieved) => {
        streamCallOrder.push('dispatch');
        processMessagePromises.push(queue.executeQuery(retrieved));
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
      streamHandlerDelay = 250;
      streamCallOrder = [];
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
        queue.executeInQueue('delay', '11', { delay: 600, result: '1' }, QueuePriority.Warmup),
        queue.executeInQueue('delay', '12', { delay: 100, result: '2' }, QueuePriority.Background),
        queue.executeInQueue('delay', '13', { delay: 100, result: '3' }, QueuePriority.Interactive)
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
      const resultPromise = queue.executeInQueue('delay', '1', { delay: 200, result: '1' }, QueuePriority.Background, {
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
      const resultPromise1 = queue.executeInQueue('delay', '31', { delay: 200, result: '1' }, QueuePriority.Interactive + 10, {
        stageQueryKey: '12',
        requestId: '4274691a-5f4c-480e-89c4-d2b9d989891c-span-1',
        spanId: 'span-id'
      });
      await delayFn(null, 50);
      const resultPromise2 = queue.executeInQueue('delay', '32', { delay: 200, result: '1' }, QueuePriority.Interactive, {
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
      // The open range between the named rungs, which is what a scheduled refresh computes
      const priority = (value: number): QueuePriority => value;

      queue.executeInQueue('delay', '31', { delay: 400, result: '4' }, priority(-10));

      await delayFn(null, 200);

      await Promise.all([
        queue.executeInQueue('delay', '32', { delay: 100, result: '3' }, priority(-9)).then(r => {
          results.push(['32', r]);
        }),
        queue.executeInQueue('delay', '33', { delay: 100, result: '2' }, priority(-8)).then(r => {
          results.push(['33', r]);
        }),
        queue.executeInQueue('delay', '34', { delay: 100, result: '1' }, priority(-7)).then(r => {
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
      const p1 = queue.executeInQueue('delay', '111', { delay: 50, result: '1' }, QueuePriority.Background);
      const p2 = delayFn(null, 50).then(() => queue.executeInQueue('delay', '112', { delay: 50, result: '2' }, QueuePriority.Background));
      const p3 = delayFn(null, 75).then(() => queue.executeInQueue('delay', '113', { delay: 50, result: '3' }, QueuePriority.Background));
      const p4 = delayFn(null, 100).then(() => queue.executeInQueue('delay', '114', { delay: 50, result: '4' }, QueuePriority.Background));

      const result = await Promise.all([p1, p2, p3, p4]);
      expect(result).toEqual(['10', '21', '32', '43']);
    });

    const onlyLocalTest = options.cacheAndQueueDriver !== 'cubestore' ? test : xtest;

    test('orphaned', async () => {
      cancelledQuery = null;

      // Two queries hold the single worker slot. orphanedTimeout keeps them out of the
      // orphaned set themselves: the memory driver reports active queries as orphaned once
      // their timeout passes, Cube Store does not.
      const pending = [
        queue.executeInQueue('delay', '121', { delay: 1200, result: '1', orphanedTimeout: 60 }, QueuePriority.Background).catch(e => e),
      ];
      await delayFn(null, 50);
      pending.push(queue.executeInQueue('delay', '122', { delay: 1200, result: '2', orphanedTimeout: 60 }, QueuePriority.Background).catch(e => e));
      await delayFn(null, 50);
      // 121 and 122 keep the worker busy for ~2.4s, so this one is still queued when its
      // 1s orphaned timeout expires
      pending.push(queue.executeInQueue('delay', '123', { delay: 50, result: '3', orphanedTimeout: 1 }, QueuePriority.Background).catch(e => e));

      // Reconciliation is what cancels orphaned queries and nothing else triggers it while
      // the worker is busy.
      const deadline = Date.now() + 2000;
      while (cancelledQuery !== '123' && Date.now() < deadline) {
        await queue.reconcileQueue();
        await delayFn(null, 100);
      }

      expect(cancelledQuery).toBe('123');

      // every client gave up on ContinueWaitError long before this point
      const outcomes = await Promise.all(pending);
      outcomes.forEach((e) => expect(e).toBeInstanceOf(ContinueWaitError));
      await awaitProcessing();

      // 123 was cancelled before the worker could pick it up
      expect(delayCount).toBe(2);
      // cancellation removed it from the queue, so the same key can be queued again
      expect(await queue.executeInQueue('delay', '123', { delay: 50, result: '3' }, QueuePriority.Background)).toBe('32');
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

    test('stream handler which starts immediately', async () => {
      streamHandlerDelay = 0;

      const key: QueryKey = ['select * from table_no_delay', []];
      key.persistent = true;
      const stream = await queue.executeInQueue('stream', key, { aliasNameToMember: {} }, 0);
      await awaitProcessing();

      // A stream which never arrived surfaces as a ContinueWaitError out of executeInQueue,
      // so reaching this line is already the assertion
      for await (const chunk of stream) {
        console.log('streaming chunk: ', chunk);
      }

      expect(streamCount).toEqual(1);
    });

    test('the stream listener is subscribed before the dispatch', async () => {
      streamHandlerDelay = 0;

      const proto = QueryQueue.prototype as any;
      const { waitForQueryStream } = proto;
      const spy = jest.spyOn(proto, 'waitForQueryStream').mockImplementation(
        function subscribeAndRecord(this: unknown, ...args: unknown[]) {
          streamCallOrder.push('subscribe');

          return waitForQueryStream.apply(this, args);
        }
      );

      try {
        const key: QueryKey = ['select * from table_ordering', []];
        key.persistent = true;
        const stream = await queue.executeInQueue('stream', key, { aliasNameToMember: {} }, QueuePriority.Background);
        await awaitProcessing();

        for await (const chunk of stream) {
          console.log('streaming chunk: ', chunk);
        }

        // Subscribing after the dispatch loses the `streamStarted` event of a handler which
        // starts fast. It is masked by the `streams` map fallback in waitForQueryStream, so
        // only the call order pins it down
        expect(streamCallOrder).toContain('subscribe');
        expect(streamCallOrder).toContain('dispatch');
        expect(streamCallOrder).toEqual(['subscribe', 'dispatch']);
      } finally {
        spy.mockRestore();
      }
    });

    test('removed before reconciled', async () => {
      const query: QueryKey = ['select * from', []];
      const key = queue.redisHash(query);
      await queue.processQuery(key, null);
      const result = await queue.executeInQueue('foo', key, query);
      expect(result).toBe('select * from bar');
    });

    onlyLocalTest('addToQueue never retrieves in memory', async () => {
      const connection = await queue.queueDriver.createConnection();
      const query: QueryKey = ['select * from add_and_retrieve', []];

      try {
        const [added, , , , retrieved] = await connection.addToQueue(
          query,
          'delay',
          { isJob: true, orphanedTimeout: undefined },
          10,
          { queueId: 1, stageQueryKey: '1', requestId: '1' }
        );

        expect(added).toBe(1);
        expect(retrieved).toBeNull();
        expect(await connection.getToProcessQueries()).toStrictEqual([
          [connection.redisHash(query), expect.any(Number)]
        ]);
      } finally {
        await connection.getQueryAndRemove(connection.redisHash(query), null);

        queue.queueDriver.release(connection);
      }
    });

    onlyLocalTest('queue driver lock obtain race condition', async () => {
      const connection: any = await queue.queueDriver.createConnection();
      const connection2: any = await queue.queueDriver.createConnection();
      const priority = 10;

      await queue.reconcileQueue();

      await connection.addToQueue(
        'race', 'handler', ['select'], priority, { stageQueryKey: 'race' }
      );

      await connection.addToQueue(
        'race2', 'handler2', ['select2'], priority, { stageQueryKey: 'race2' }
      );

      const processingId1 = await connection.getNextProcessingId();
      const processingId4 = await connection.getNextProcessingId();

      await connection.freeProcessingLock('race', processingId1, true);
      await connection.freeProcessingLock('race2', processingId4, true);

      await connection2.retrieveForProcessing('race2', await connection.getNextProcessingId());

      const processingId = await connection.getNextProcessingId();
      const retrieve6 = await connection.retrieveForProcessing('race', processingId);
      console.log(retrieve6);
      expect(!!retrieve6[5]).toBe(true);

      console.log(await connection.getQueryAndRemove('race'));
      console.log(await connection.getQueryAndRemove('race2'));

      await queue.queueDriver.release(connection);
      await queue.queueDriver.release(connection2);
    });

    onlyLocalTest('activated but lock is not acquired', async () => {
      const connection = await queue.queueDriver.createConnection();
      const connection2 = await queue.queueDriver.createConnection();
      const priority = 10;

      await queue.reconcileQueue();

      await connection.addToQueue(
        'activated1', 'handler', <any>['select'], priority, { stageQueryKey: 'race', requestId: '1' }
      );

      await connection.addToQueue(
        'activated2', 'handler2', <any>['select2'], priority, { stageQueryKey: 'race2', requestId: '1' }
      );

      const processingId1 = await connection.getNextProcessingId();
      const processingId2 = await connection.getNextProcessingId();
      const processingId3 = await connection.getNextProcessingId();

      const retrieve1 = await connection.retrieveForProcessing('activated1' as any, processingId1);
      console.log(retrieve1);
      const retrieve2 = await connection2.retrieveForProcessing('activated2' as any, processingId2);
      console.log(retrieve2);
      console.log(await connection.freeProcessingLock('activated1' as any, processingId1, retrieve1 && retrieve1[2].indexOf('activated1' as any) !== -1));
      const retrieve3 = await connection.retrieveForProcessing('activated2' as any, processingId3);
      console.log(retrieve3);
      console.log(await connection.freeProcessingLock('activated2' as any, processingId3, retrieve3 && retrieve3[2].indexOf('activated2' as any) !== -1));
      console.log(retrieve2[2].indexOf('activated2' as any) !== -1);
      console.log(await connection2.freeProcessingLock('activated2' as any, processingId2, retrieve2 && retrieve2[2].indexOf('activated2' as any) !== -1));

      const retrieve4 = await connection.retrieveForProcessing('activated2' as any, await connection.getNextProcessingId());
      console.log(retrieve4);
      expect(retrieve4[0]).toBe(1);
      expect(!!retrieve4[5]).toBe(true);

      console.log(await connection.getQueryAndRemove('activated1' as any, null));
      console.log(await connection.getQueryAndRemove('activated2' as any, null));

      await queue.queueDriver.release(connection);
      await queue.queueDriver.release(connection2);
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
          .executeInQueue('delay', query, { delay: 1500, result: '1' }, QueuePriority.Background, {
            stageQueryKey: query, requestId: '70b0b0a6-60ff-43ee-95ca-b5a3d864879f-span-1', spanId: 'span-A'
          })
          .catch(e => e);
        const clientB = queue
          .executeInQueue('delay', query, { delay: 1500, result: '1' }, QueuePriority.Background, {
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
          queue.executeInQueue('delay', query, { delay: 1500, result: '1' }, QueuePriority.Background, {
            stageQueryKey: query, requestId: '70b0b0a6-60ff-43ee-95ca-b5a3d864879f-span-2', spanId: 'span-A2'
          }),
          queue.executeInQueue('delay', query, { delay: 1500, result: '1' }, QueuePriority.Background, {
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
            result = await queue.executeInQueue('delay', query, { delay: 1500, result: '1' }, QueuePriority.Background, {
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
          const secondResult = await queue.executeInQueue('delay', query, { delay: 1500, result: '1' }, QueuePriority.Background, {
            stageQueryKey: query,
            requestId: `${requestUuid}-span-${spanCounter++}`,
            spanId: `span-${spanCounter}`,
          });
          expect(secondResult).toBeDefined();
          expect(delayCount).toBe(1);
        }
      }, 30000);
    });

    // eslint-disable-next-line no-unused-expressions
    options.cacheAndQueueDriver === 'cubestore' && describe('with CUBEJS_QUEUE_FAST_TRACK enabled', () => {
      jest.setTimeout(10 * 1000);

      beforeAll(() => {
        process.env.CUBEJS_QUEUE_FAST_TRACK = 'true';
      });

      afterAll(() => {
        delete process.env.CUBEJS_QUEUE_FAST_TRACK;
      });

      test('an idle queue retrieves the query on add', async () => {
        const retrieveForProcessing = jest.spyOn(CubestoreQueueDriverConnection.prototype, 'retrieveForProcessing');
        const driverQuery = jest.spyOn(CubeStoreDriver.prototype, 'query');

        try {
          const query: QueryKey = ['select * from fast_track', []];
          const result = await queue.executeInQueue('foo', query, query, QueuePriority.Interactive);

          expect(result).toBe('select * from fast_track bar');
          expect(driverQuery.mock.calls.some(([sql]) => sql.startsWith('QUEUE ADD_AND_RETRIEVE'))).toBe(true);
          // The retrieval came with the insert, there was nothing left to retrieve
          expect(retrieveForProcessing).not.toHaveBeenCalled();
          // The retrieval carries the processing identity, the acknowledgement must be accepted
          expect(logger.mock.calls.map(([message]) => message)).not.toContain('Orphaned execution result');
        } finally {
          retrieveForProcessing.mockRestore();
          driverQuery.mockRestore();
        }
      });

      test('concurrent clients execute the query once', async () => {
        const results = await Promise.all([
          queue.executeInQueue('delay', 'fast_track_concurrent', { delay: 400, result: '2' }, QueuePriority.Interactive),
          queue.executeInQueue('delay', 'fast_track_concurrent', { delay: 400, result: '2' }, QueuePriority.Interactive)
        ]);

        expect(results).toStrictEqual(['20', '20']);
        expect(delayCount).toBe(1);
      });

      test('a background priority query takes the normal path', async () => {
        const driverQuery = jest.spyOn(CubeStoreDriver.prototype, 'query');

        try {
          const query: QueryKey = ['select * from slow_track', []];
          const result = await queue.executeInQueue('foo', query, query, QueuePriority.Interactive - 1);

          expect(result).toBe('select * from slow_track bar');
          expect(driverQuery.mock.calls.some(([sql]) => sql.startsWith('QUEUE ADD_AND_RETRIEVE'))).toBe(false);
          expect(driverQuery.mock.calls.some(([sql]) => sql.startsWith('QUEUE ADD PRIORITY'))).toBe(true);
        } finally {
          driverQuery.mockRestore();
        }
      });

      test('a query is not retrieved while the concurrency budget is taken', async () => {
        const connection = await queue.queueDriver.createConnection();
        const first: QueryKey = ['select * from budget_1', []];
        const second: QueryKey = ['select * from budget_2', []];
        const addToQueue = (queryKey: QueryKey, queueId: number) => connection.addToQueue(
          queryKey,
          'delay',
          { isJob: true, orphanedTimeout: undefined },
          QueuePriority.Interactive,
          { queueId, stageQueryKey: `${queueId}`, requestId: `${queueId}` }
        );

        try {
          // concurrency is 1, the first query takes the only slot
          const [added1, , , , retrieved1] = await addToQueue(first, 1);
          expect(added1).toBe(1);
          expect(retrieved1?.[5]).toBe(true);
          expect(retrieved1?.[4].queryKey).toStrictEqual(first);

          // an active item is never retrieved twice
          const [added1again, , , , retrieved1again] = await addToQueue(first, 1);
          expect(added1again).toBe(0);
          expect(retrieved1again).toBeNull();

          const [added2, , , , retrieved2] = await addToQueue(second, 2);
          expect(added2).toBe(1);
          expect(retrieved2).toBeNull();

          // A retrieved item goes straight to active and never becomes pending, the one
          // which was not retrieved is left for reconcile to pick up by priority
          expect(await connection.getActiveQueries()).toStrictEqual([
            [connection.redisHash(first), expect.any(Number)]
          ]);
          expect(await connection.getToProcessQueries()).toStrictEqual([
            [connection.redisHash(second), expect.any(Number)]
          ]);
        } finally {
          await connection.getQueryAndRemove(connection.redisHash(first), null);
          await connection.getQueryAndRemove(connection.redisHash(second), null);

          queue.queueDriver.release(connection);
        }
      });

      test('a failing dispatch does not surface to the client', async () => {
        const query: QueryKey = ['select * from dispatch_failure', []];
        const connection = await queue.queueDriver.createConnection();
        // The retrieval already made the item active, so a throwing dispatch must be logged and
        // left to the heartbeat reclaim rather than failing the request the way `processQuery`
        // would never fail it
        const sendProcessMessage = jest.spyOn(queue as any, 'sendProcessMessageFn')
          .mockRejectedValueOnce(new Error('the worker is gone'));

        try {
          await expect(
            queue.executeInQueue('foo', query, query, QueuePriority.Interactive)
          ).rejects.toBeInstanceOf(ContinueWaitError);

          expect(logger.mock.calls.map(([message]) => message)).toContain('Error while processing message');
        } finally {
          sendProcessMessage.mockRestore();
          // The reclaim only comes after heartBeatTimeout, too late for the suite to wait for
          await connection.getQueryAndRemove(connection.redisHash(query), null);

          queue.queueDriver.release(connection);
        }
      });
    });
  });
};
