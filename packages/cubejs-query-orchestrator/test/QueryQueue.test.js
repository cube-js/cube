/* globals describe, test, expect, afterAll */
const QueryQueue = require('../orchestrator/QueryQueue');
const RedisPool = require('../orchestrator/RedisPool');

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
  });
};

QueryQueueTest('Local');
QueryQueueTest('RedisPool', { cacheAndQueueDriver: 'redis', redisPool: new RedisPool() });
QueryQueueTest('RedisNoPool', { cacheAndQueueDriver: 'redis', redisPool: new RedisPool({ poolMin: 0, poolMax: 0 }) });
