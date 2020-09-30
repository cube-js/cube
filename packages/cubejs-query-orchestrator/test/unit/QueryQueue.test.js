/* globals describe, test, expect, afterAll */
const QueryQueue = require('../../orchestrator/QueryQueue');

const QueryQueueTest = (name, options) => {
  describe(`QueryQueue${name}`, () => {
    let delayCount = 0;
    const delayFn = (result, delay) => new Promise(resolve => setTimeout(() => resolve(result), delay));
    let cancelledQuery;
    const queue = new QueryQueue('test_query_queue', {
      queryHandlers: {
        foo: async (query) => `${query[0]} bar`,
        delay: async (query, setCancelHandler) => {
          const result = query.result + delayCount;
          delayCount += 1;
          await setCancelHandler(result);
          return delayFn(result, query.delay);
        }
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
      ...options
    });

    afterAll(async () => {
      await options.redisPool.cleanup();
    });

    test('gutter', async () => {
      const query = ['select * from'];
      const result = await queue.executeInQueue('foo', query, query);
      expect(result).toBe('select * from bar');
    });

    test('instant double wait resolve', async () => {
      const results = await Promise.all([
        queue.executeInQueue('delay', `instant`, { delay: 400, result: '2' }),
        queue.executeInQueue('delay', `instant`, { delay: 400, result: '2' })
      ]);
      expect(results).toStrictEqual(['20', '20']);
    });

    test('priority', async () => {
      delayCount = 0;
      const result = await Promise.all([
        queue.executeInQueue('delay', `11`, { delay: 600, result: '1' }, 1),
        queue.executeInQueue('delay', `12`, { delay: 100, result: '2' }, 0),
        queue.executeInQueue('delay', `13`, { delay: 100, result: '3' }, 10)
      ]);
      expect(parseInt(result.find(f => f[0] === '3'), 10) % 10).toBeLessThan(2);
    });


    test('timeout', async () => {
      delayCount = 0;
      const query = ['select * from 2'];
      let errorString = '';
      for (let i = 0; i < 5; i++) {
        try {
          await queue.executeInQueue('delay', query, { delay: 3000, result: '1' });
          console.log(`Delay ${i}`);
        } catch (e) {
          if (e.message === 'Continue wait') {
            // eslint-disable-next-line no-continue
            continue;
          }
          errorString = e.toString();
          break;
        }
      }
      expect(errorString).toEqual(expect.stringContaining('timeout'));
    });


    test('stage reporting', async () => {
      delayCount = 0;
      const resultPromise = queue.executeInQueue('delay', '1', { delay: 200, result: '1' }, 0, { stageQueryKey: '1' });
      await delayFn(null, 50);
      expect((await queue.getQueryStage('1')).stage).toBe('Executing query');
      await resultPromise;
      expect(await queue.getQueryStage('1')).toEqual(undefined);
    });

    test('priority stage reporting', async () => {
      delayCount = 0;
      const resultPromise = queue.executeInQueue('delay', '31', { delay: 200, result: '1' }, 20, { stageQueryKey: '12' });
      await delayFn(null, 50);
      const resultPromise2 = queue.executeInQueue('delay', '32', { delay: 200, result: '1' }, 10, { stageQueryKey: '12' });
      await delayFn(null, 50);
      expect((await queue.getQueryStage('12', 10)).stage).toBe('#1 in queue');
      await resultPromise;
      await resultPromise2;
      expect(await queue.getQueryStage('12')).toEqual(undefined);
    });

    test('negative priority', async () => {
      delayCount = 0;
      const results = [];
      await Promise.all([
        queue.executeInQueue('delay', '31', { delay: 400, result: '4' }, -10).then(r => results.push(r)),
        queue.executeInQueue('delay', '32', { delay: 100, result: '3' }, -9).then(r => results.push(r)),
        queue.executeInQueue('delay', '33', { delay: 100, result: '2' }, -8).then(r => results.push(r)),
        queue.executeInQueue('delay', '34', { delay: 100, result: '1' }, -7).then(r => results.push(r))
      ]);

      results.splice(0, 1);

      expect(results.map(r => parseInt(r[0], 10) - parseInt(results[0][0], 10))).toEqual([0, 1, 2]);
    });

    test('orphaned', async () => {
      for (let i = 1; i <= 4; i++) {
        await queue.executeInQueue('delay', `11${i}`, { delay: 50, result: `${i}` }, 0);
      }
      cancelledQuery = null;
      delayCount = 0;

      let result = queue.executeInQueue('delay', `111`, { delay: 800, result: '1' }, 0);
      delayFn(null, 50).then(() => queue.executeInQueue('delay', `112`, { delay: 800, result: '2' }, 0)).catch(e => e);
      delayFn(null, 60).then(() => queue.executeInQueue('delay', `113`, { delay: 500, result: '3' }, 0)).catch(e => e);
      delayFn(null, 70).then(() => queue.executeInQueue('delay', `114`, { delay: 900, result: '4' }, 0)).catch(e => e);

      expect(await result).toBe('10');
      await queue.executeInQueue('delay', `112`, { delay: 800, result: '2' }, 0);
      result = await queue.executeInQueue('delay', `113`, { delay: 900, result: '3' }, 0);
      expect(result).toBe('32');
      await delayFn(null, 200);
      expect(cancelledQuery).toBe('114');
      await queue.executeInQueue('delay', `114`, { delay: 50, result: '4' }, 0);
    });

    test('removed before reconciled', async () => {
      const query = ['select * from'];
      await queue.processQuery(query);
      const result = await queue.executeInQueue('foo', query, query);
      expect(result).toBe('select * from bar');
    });

    test('queue driver lock obtain race condition', async () => {
      const redisClient = await queue.queueDriver.createConnection();
      const redisClient2 = await queue.queueDriver.createConnection();
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

    test('activated but lock is not acquired', async () => {
      const redisClient = await queue.queueDriver.createConnection();
      const redisClient2 = await queue.queueDriver.createConnection();
      const priority = 10;
      const time = new Date().getTime();
      const keyScore = time + (10000 - priority) * 1E14;

      await queue.reconcileQueue();

      await redisClient.addToQueue(
        keyScore, 'activated1', time, 'handler', ['select'], priority, { stageQueryKey: 'race' }
      );

      await redisClient.addToQueue(
        keyScore + 100, 'activated2', time + 100, 'handler2', ['select2'], priority, { stageQueryKey: 'race2' }
      );

      const processingId1 = await redisClient.getNextProcessingId();
      const processingId2 = await redisClient.getNextProcessingId();
      const processingId3 = await redisClient.getNextProcessingId();

      const retrieve1 = await redisClient.retrieveForProcessing('activated1', processingId1);
      console.log(retrieve1);
      const retrieve2 = await redisClient2.retrieveForProcessing('activated2', processingId2);
      console.log(retrieve2);
      console.log(await redisClient.freeProcessingLock('activated1', processingId1, retrieve1 && retrieve1[2].indexOf('activated1') !== -1));
      const retrieve3 = await redisClient.retrieveForProcessing('activated2', processingId3);
      console.log(retrieve3);
      console.log(await redisClient.freeProcessingLock('activated2', processingId3, retrieve3 && retrieve3[2].indexOf('activated2') !== -1));
      console.log(retrieve2[2].indexOf('activated2') !== -1);
      console.log(await redisClient2.freeProcessingLock('activated2', processingId2, retrieve2 && retrieve2[2].indexOf('activated2') !== -1));

      const retrieve4 = await redisClient.retrieveForProcessing('activated2', await redisClient.getNextProcessingId());
      console.log(retrieve4);
      expect(retrieve4[0]).toBe(1);
      expect(!!retrieve4[5]).toBe(true);

      console.log(await redisClient.getQueryAndRemove('activated1'));
      console.log(await redisClient.getQueryAndRemove('activated2'));

      await queue.queueDriver.release(redisClient);
      await queue.queueDriver.release(redisClient2);
    });
  });
};

QueryQueueTest('Local');

module.exports = QueryQueueTest;
