import { CubeStoreDriver, CubeStoreQueueDriver } from '@cubejs-backend/cubestore-driver';
import crypto from 'crypto';
import { createPromiseLock, pausePromise } from '@cubejs-backend/shared';
import { QueueDriverConnectionInterface, QueueDriverInterface, } from '@cubejs-backend/base-driver';
import { LocalQueueDriver, QueryQueue } from '../../src';

export type QueryQueueTestOptions = {
  cacheAndQueueDriver?: string,
  redisPool?: any,
  cubeStoreDriverFactory?: () => Promise<CubeStoreDriver>,
  beforeAll?: () => Promise<void>,
  afterAll?: () => Promise<void>,
};

function patchQueueDriverConnectionForTrack(connection: QueueDriverConnectionInterface, counters: any): QueueDriverConnectionInterface {
  function wrapAsyncMethod(methodName: string): any {
    return async function (...args) {
      if (!(methodName in counters.methods)) {
        counters.methods[methodName] = {
          started: 1,
          finished: 0,
        };
      } else {
        counters.methods[methodName].started++;
      }

      const result = await connection[methodName](...args);
      counters.methods[methodName].finished++;

      return result;
    };
  }

  return {
    ...connection,
    addToQueue: wrapAsyncMethod('addToQueue'),
    getResult: wrapAsyncMethod('getResult'),
    getQueriesToCancel: wrapAsyncMethod('getQueriesToCancel'),
    getActiveAndToProcess: wrapAsyncMethod('getActiveAndToProcess'),
    retrieveForProcessing: wrapAsyncMethod('retrieveForProcessing'),
    getQueryDef: wrapAsyncMethod('getQueryDef'),
    setResultAndRemoveQuery: wrapAsyncMethod('setResultAndRemoveQuery'),
    getQueryStageState: wrapAsyncMethod('getQueryStageState'),
    getResultBlocking: wrapAsyncMethod('getResultBlocking'),
    freeProcessingLock: wrapAsyncMethod('freeProcessingLock'),
    optimisticQueryUpdate: wrapAsyncMethod('optimisticQueryUpdate'),
    getQueryAndRemove: wrapAsyncMethod('getQueryAndRemove'),
    getNextProcessingId: wrapAsyncMethod('getNextProcessingId'),
    release: connection.release,
  };
}

function patchQueueDriverForTrack(driver: QueueDriverInterface, counters: any): QueueDriverInterface {
  return {
    ...driver,
    createConnection: async () => {
      counters.connections++;

      return patchQueueDriverConnectionForTrack(await driver.createConnection(), counters);
    },
    redisHash: (...args) => driver.redisHash(...args),
    release: async (...args) => {
      counters.connections--;

      return driver.release(...args);
    },
  };
}

export function QueryQueueBenchmark(name: string, options: QueryQueueTestOptions = {}) {
  (async () => {
    if (options.beforeAll) {
      await options.beforeAll();
    }

    const createBenchmark = async (benchSettings: { totalQueries: number, queueResponseSize: number, queuePayloadSize: number, currency: number }) => {
      const counters = {
        connections: 0,
        methods: {},
        events: {},
        queueStarted: 0,
        queueResolved: 0,
        handlersStarted: 0,
        handlersFinished: 0,
        queueDriverQueriesStarted: 0,
      };

      const queueDriverFactory = (driverType, queueDriverOptions) => {
        switch (driverType) {
          case 'memory':
            return patchQueueDriverForTrack(
              new LocalQueueDriver(
                queueDriverOptions
              ) as any,
              counters
            );
          case 'cubestore':
            return patchQueueDriverForTrack(
              new CubeStoreQueueDriver(
                async () => options.cubeStoreDriverFactory(),
                queueDriverOptions
              ),
              counters
            );
          default:
            throw new Error(`Unsupported driver: ${driverType}`);
        }
      };

      const tenantPrefix = crypto.randomBytes(6).toString('hex');
      const queue = new QueryQueue(`${tenantPrefix}#test_query_queue`, {
        queryHandlers: {
          query: async (_query) => {
            counters.handlersStarted++;
            await pausePromise(1500);
            counters.handlersFinished++;

            return {
              payload: 'a'.repeat(benchSettings.queueResponseSize),
            };
          },
        },
        continueWaitTimeout: 60 * 2,
        executionTimeout: 20,
        orphanedTimeout: 60 * 5,
        concurrency: benchSettings.currency,
        logger: (event, _params) => {
          // console.log(event, _params);
          // console.log(event);

          if (event in counters.events) {
            counters.events[event]++;
          } else {
            counters.events[event] = 1;
          }
        },
        queueDriverFactory,
        ...options
      });

      const processingPromisses = [];

      async function awaitProcessing() {
        // process query can call reconcileQueue
        while (await queue.shutdown() || processingPromisses.length) {
          console.log('awaitProcessing', {
            counters,
            processingPromisses: processingPromisses.length
          });
          await Promise.all(processingPromisses.splice(0));
        }
      }

      const progressIntervalId = setInterval(() => {
        console.log('running', {
          ...counters,
          processingPromisses: processingPromisses.length
        });
      }, 1000);

      const lock = createPromiseLock();

      const pusherIntervalId = setInterval(async () => {
        if (counters.queueStarted >= benchSettings.totalQueries) {
          lock.resolve();
          clearInterval(pusherIntervalId);

          return;
        }

        counters.queueStarted++;

        const queueId = crypto.randomBytes(12).toString('hex');
        const running = (async () => {
          await queue.executeInQueue('query', queueId, {
            // eslint-disable-next-line no-bitwise
            payload: 'a'.repeat(benchSettings.queuePayloadSize)
          }, 1, {

          });

          counters.queueResolved++;

          // loosing memory for result
          return null;
        })();

        processingPromisses.push(running);
        await running;
      }, 10);

      await lock.promise;
      await awaitProcessing();
      clearInterval(progressIntervalId);

      console.log('Result', {
        benchSettings,
        ...counters,
      });
    };

    await createBenchmark({
      currency: 50,
      totalQueries: 1_000,
      // eslint-disable-next-line no-bitwise
      queueResponseSize: 5 << 20,
      queuePayloadSize: 256 * 1024,
    });

    if (options.afterAll) {
      await options.afterAll();
    }
  })();
}
