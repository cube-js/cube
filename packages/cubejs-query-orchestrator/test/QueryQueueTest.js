const QueryQueue = require('../orchestrator/QueryQueue');
const should = require('should');
const redis = require('redis');

const QueryQueueTest = (name, options) => {
  describe(`QueryQueue${name}`, function () {
    this.timeout(5000);

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
          return await delayFn(result, query.delay);
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

    it('gutter', async () => {
      const query = ['select * from'];
      const result = await queue.executeInQueue('foo', query, query);
      should(result).be.eql('select * from bar');
    });

    it('priority', async () => {
      delayCount = 0;
      const result = await Promise.all([
        queue.executeInQueue('delay', `11`, { delay: 200, result: '1' }, 1),
        queue.executeInQueue('delay', `12`, { delay: 300, result: '2' }, 0),
        queue.executeInQueue('delay', `13`, { delay: 400, result: '3' }, 10)
      ]);
      should(result).be.eql(['11', '22', '30']);
    });

    it('timeout', async () => {
      delayCount = 0;
      const query = ['select * from 2'];
      let errorString = '';
      for (let i = 0; i < 5; i++) {
        try {
          await queue.executeInQueue('delay', query, { delay: 3000, result: '1' });
        } catch (e) {
          if (e.message === 'Continue wait') {
            continue;
          }
          errorString = e.toString();
          break;
        }
      }
      should(errorString.indexOf('timeout')).not.be.eql(-1);
    });

    it('stage reporting', async () => {
      delayCount = 0;
      const resultPromise = queue.executeInQueue('delay', '1', { delay: 50, result: '1' }, 0, { stageQueryKey: '1' });
      await delayFn(null, 10);
      should((await queue.getQueryStage('1')).stage).be.eql('Executing query');
      await resultPromise;
      should(await queue.getQueryStage('1')).be.eql(undefined);
    });

    it('priority stage reporting', async () => {
      delayCount = 0;
      const resultPromise = queue.executeInQueue('delay', '31', { delay: 100, result: '1' }, 20, { stageQueryKey: '12' });
      await delayFn(null, 10);
      const resultPromise2 = queue.executeInQueue('delay', '32', { delay: 100, result: '1' }, 10, { stageQueryKey: '12' });
      await delayFn(null, 10);
      should((await queue.getQueryStage('12', 10)).stage).be.eql('#1 in queue');
      await resultPromise;
      await resultPromise2;
      should(await queue.getQueryStage('12')).be.eql(undefined);
    });

    it('orphaned', async () => {
      for (let i = 1; i <= 4; i++) {
        await queue.executeInQueue('delay', `11` + i, { delay: 50, result: '' + i }, 0);
      }
      cancelledQuery = null;
      delayCount = 0;

      let result = queue.executeInQueue('delay', `111`, { delay: 800, result: '1' }, 0);
      delayFn(null, 50).then(() => queue.executeInQueue('delay', `112`, { delay: 800, result: '2' }, 0)).catch(e => e);
      delayFn(null, 60).then(() => queue.executeInQueue('delay', `113`, { delay: 500, result: '3' }, 0)).catch(e => e);
      delayFn(null, 70).then(() => queue.executeInQueue('delay', `114`, { delay: 900, result: '4' }, 0)).catch(e => e);

      should(await result).be.eql('10');
      await queue.executeInQueue('delay', `112`, { delay: 800, result: '2' }, 0);
      result = await queue.executeInQueue('delay', `113`, { delay: 900, result: '3' }, 0);
      should(result).be.eql('32');
      await delayFn(null, 200);
      should(cancelledQuery).be.eql('114');
      await queue.executeInQueue('delay', `114`, { delay: 50, result: '4' }, 0);
    });
  });
};

QueryQueueTest('Local');
QueryQueueTest('Redis', { createRedisClient: () => redis.createClient() });
