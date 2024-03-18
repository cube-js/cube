import { Readable } from 'stream';
import { CubeStoreDriver } from '@cubejs-backend/cubestore-driver';
import type { QueryKey } from '@cubejs-backend/base-driver';
import { pausePromise } from '@cubejs-backend/shared';
import crypto from 'crypto';

import { QueryQueue } from '../../src';
import { processUidRE } from '../../src/orchestrator/utils';

export type QueryQueueTestOptions = {
  cacheAndQueueDriver?: string,
  redisPool?: any,
  cubeStoreDriverFactory?: () => Promise<CubeStoreDriver>,
  beforeAll?: () => Promise<void>,
  afterAll?: () => Promise<void>,
};

export const QueryQueueTest = (name: string, options: QueryQueueTestOptions = {}) => {
  describe(`QueryQueue${name}`, () => {
    jest.setTimeout(10 * 1000);

    const delayFn = (result, delay) => new Promise(resolve => setTimeout(() => resolve(result), delay));
    const logger = jest.fn((message, event) => console.log(`${message} ${JSON.stringify(event)}`));

    let delayCount = 0;
    let streamCount = 0;
    const processMessagePromises: Promise<any>[] = [];
    const processCancelPromises: Promise<any>[] = [];
    let cancelledQuery;

    const tenantPrefix = crypto.randomBytes(6).toString('hex');

    const queue = new QueryQueue(`${tenantPrefix}#test_query_queue`, {
      queryHandlers: {
        foo: async (query) => `${query[0]} bar`,
        delay: async (query, setCancelHandler) => {
          const result = query.result + delayCount;
          delayCount += 1;
          await setCancelHandler(result);
          return delayFn(result, query.delay);
        },
        stream: async (query, stream) => {
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
      },
      sendProcessMessageFn: async (queryKeyHashed, queueId) => {
        processMessagePromises.push(queue.processQuery.bind(queue)(queryKeyHashed, queueId));
      },
      sendCancelMessageFn: async (query) => {
        processCancelPromises.push(queue.processCancel.bind(queue)(query));
      },
      cancelHandlers: {
        delay: (query) => {
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

      if (options.redisPool) {
        await options.redisPool.cleanup();
      }

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
      const query = ['select * from'];
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
      const query = ['select * from 2'];
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
      const query = ['select * from 3'];

      // executionTimeout is 2s, 5s is enough
      await queue.executeInQueue('delay', query, { delay: 5 * 1000, result: '1', isJob: true });
      await awaitProcessing();

      expect(logger.mock.calls.length).toEqual(5);
      // assert that query queue is able to get query def by query key
      expect(logger.mock.calls[4][0]).toEqual('Cancelling query due to timeout');
      expect(logger.mock.calls[3][0]).toEqual('Error while querying');
    });

    test('stage reporting', async () => {
      const resultPromise = queue.executeInQueue('delay', '1', { delay: 200, result: '1' }, 0, { stageQueryKey: '1' });
      await delayFn(null, 50);
      expect((await queue.getQueryStage('1')).stage).toBe('Executing query');
      await resultPromise;
      expect(await queue.getQueryStage('1')).toEqual(undefined);
    });

    test('priority stage reporting', async () => {
      const resultPromise1 = queue.executeInQueue('delay', '31', { delay: 200, result: '1' }, 20, { stageQueryKey: '12' });
      await delayFn(null, 50);
      const resultPromise2 = queue.executeInQueue('delay', '32', { delay: 200, result: '1' }, 10, { stageQueryKey: '12' });
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

    const nonCubeStoreTest = options.cacheAndQueueDriver !== 'cubestore' ? test : xtest;

    // this works with cube store, but there is an issue with timings
    // TODO(ovr): fix me
    nonCubeStoreTest('orphaned', async () => {
      // recover if previous test broken something
      for (let i = 1; i <= 4; i++) {
        await queue.executeInQueue('delay', `11${i}`, { delay: 50, result: `${i}` }, 0);
      }

      cancelledQuery = null;
      delayCount = 0;

      let result = queue.executeInQueue('delay', '111', { delay: 800, result: '1' }, 0);
      delayFn(null, 50).then(() => queue.executeInQueue('delay', '112', { delay: 800, result: '2' }, 0)).catch(e => e);
      delayFn(null, 75).then(() => queue.executeInQueue('delay', '113', { delay: 500, result: '3' }, 0)).catch(e => e);
      // orphaned timeout should be applied
      delayFn(null, 100).then(() => queue.executeInQueue('delay', '114', { delay: 900, result: '4' }, 0)).catch(e => e);

      expect(await result).toBe('10');
      await queue.executeInQueue('delay', '112', { delay: 800, result: '2' }, 0);

      result = await queue.executeInQueue('delay', '113', { delay: 900, result: '3' }, 0);
      expect(result).toBe('32');

      await delayFn(null, 200);
      expect(cancelledQuery).toBe('114');
      await queue.executeInQueue('delay', '114', { delay: 50, result: '4' }, 0);
    });

    test('orphaned with custom ttl', async () => {
      const connection = await queue.queueDriver.createConnection();

      try {
        const priority = 10;
        const time = new Date().getTime();
        const keyScore = time + (10000 - priority) * 1E14;

        expect(await connection.getOrphanedQueries()).toEqual([]);

        let orphanedTimeout = 2;
        await connection.addToQueue(keyScore, ['1', []], time + (orphanedTimeout * 1000), 'delay', { isJob: true, orphanedTimeout: time, }, priority, {
          queueId: 1,
          stageQueryKey: '1',
          requestId: '1',
          orphanedTimeout,
        });

        expect(await connection.getOrphanedQueries()).toEqual([]);

        orphanedTimeout = 60;

        await connection.addToQueue(keyScore, ['2', []], time + (orphanedTimeout * 1000), 'delay', { isJob: true, orphanedTimeout: time, }, priority, {
          queueId: 2,
          stageQueryKey: '2',
          requestId: '2',
          orphanedTimeout,
        });

        await pausePromise(2000 + 500 /*  additional timeout on CI */);

        expect(await connection.getOrphanedQueries()).toEqual([
          [
            connection.redisHash(['1', []]),
            // Redis doesnt support queueId, it will return Null
            name.includes('Redis') ? null : expect.any(Number)
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

    nonCubeStoreTest('queue driver lock obtain race condition', async () => {
      const redisClient: any = await queue.queueDriver.createConnection();
      const redisClient2: any = await queue.queueDriver.createConnection();
      const priority = 10;
      const time = new Date().getTime();
      const keyScore = time + (10000 - priority) * 1E14;

      // console.log(await redisClient.getQueryAndRemove('race'));
      // console.log(await redisClient.getQueryAndRemove('race1'));

      if (redisClient.redisClient) {
        await redisClient2.redisClient.setAsync(redisClient.queryProcessingLockKey('race'), '100');
        await redisClient.redisClient.watchAsync(redisClient.queryProcessingLockKey('race'));
        await redisClient2.redisClient.setAsync(redisClient.queryProcessingLockKey('race'), Math.random());

        const res = await redisClient.redisClient.multi()
          .set(redisClient.queryProcessingLockKey('race'), '100')
          .set(redisClient.queryProcessingLockKey('race1'), '100')
          .execAsync();

        expect(res).toBe(null);
        await redisClient.redisClient.delAsync(redisClient.queryProcessingLockKey('race'));
        await redisClient.redisClient.delAsync(redisClient.queryProcessingLockKey('race1'));
      }

      await queue.reconcileQueue();

      await redisClient.addToQueue(
        keyScore, 'race', time, 'handler', ['select'], priority, { stageQueryKey: 'race' }
      );

      await redisClient.addToQueue(
        keyScore + 100, 'race2', time + 100, 'handler2', ['select2'], priority, { stageQueryKey: 'race2' }
      );

      const processingId1 = await redisClient.getNextProcessingId();
      const processingId4 = await redisClient.getNextProcessingId();

      await redisClient.freeProcessingLock('race', processingId1, true);
      await redisClient.freeProcessingLock('race2', processingId4, true);

      await redisClient2.retrieveForProcessing('race2', await redisClient.getNextProcessingId());

      const processingId = await redisClient.getNextProcessingId();
      const retrieve6 = await redisClient.retrieveForProcessing('race', processingId);
      console.log(retrieve6);
      expect(!!retrieve6[5]).toBe(true);

      console.log(await redisClient.getQueryAndRemove('race'));
      console.log(await redisClient.getQueryAndRemove('race2'));

      await queue.queueDriver.release(redisClient);
      await queue.queueDriver.release(redisClient2);
    });

    nonCubeStoreTest('activated but lock is not acquired', async () => {
      const redisClient = await queue.queueDriver.createConnection();
      const redisClient2 = await queue.queueDriver.createConnection();
      const priority = 10;
      const time = new Date().getTime();
      const keyScore = time + (10000 - priority) * 1E14;

      await queue.reconcileQueue();

      await redisClient.addToQueue(
        keyScore, 'activated1', time, 'handler', <any>['select'], priority, { stageQueryKey: 'race', requestId: '1', queueId: 1 }
      );

      await redisClient.addToQueue(
        keyScore + 100, 'activated2', time + 100, 'handler2', <any>['select2'], priority, { stageQueryKey: 'race2', requestId: '1', queueId: 2 }
      );

      const processingId1 = await redisClient.getNextProcessingId();
      const processingId2 = await redisClient.getNextProcessingId();
      const processingId3 = await redisClient.getNextProcessingId();

      const retrieve1 = await redisClient.retrieveForProcessing('activated1' as any, processingId1);
      console.log(retrieve1);
      const retrieve2 = await redisClient2.retrieveForProcessing('activated2' as any, processingId2);
      console.log(retrieve2);
      console.log(await redisClient.freeProcessingLock('activated1' as any, processingId1, retrieve1 && retrieve1[2].indexOf('activated1' as any) !== -1));
      const retrieve3 = await redisClient.retrieveForProcessing('activated2' as any, processingId3);
      console.log(retrieve3);
      console.log(await redisClient.freeProcessingLock('activated2' as any, processingId3, retrieve3 && retrieve3[2].indexOf('activated2' as any) !== -1));
      console.log(retrieve2[2].indexOf('activated2' as any) !== -1);
      console.log(await redisClient2.freeProcessingLock('activated2' as any, processingId2, retrieve2 && retrieve2[2].indexOf('activated2' as any) !== -1));

      const retrieve4 = await redisClient.retrieveForProcessing('activated2' as any, await redisClient.getNextProcessingId());
      console.log(retrieve4);
      expect(retrieve4[0]).toBe(1);
      expect(!!retrieve4[5]).toBe(true);

      console.log(await redisClient.getQueryAndRemove('activated1' as any, null));
      console.log(await redisClient.getQueryAndRemove('activated2' as any, null));

      await queue.queueDriver.release(redisClient);
      await queue.queueDriver.release(redisClient2);
    });
  });
};
