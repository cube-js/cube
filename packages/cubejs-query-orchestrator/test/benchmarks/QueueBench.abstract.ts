import { CubeStoreQueueDriver } from '@cubejs-backend/cubestore-driver';
import crypto from 'crypto';
import path from 'path';
import { ChildProcess, fork } from 'child_process';
import { createPromiseLock, MethodName, pausePromise } from '@cubejs-backend/shared';
import { QueueDriverConnectionInterface, QueueDriverInterface, } from '@cubejs-backend/base-driver';
import { LocalQueueDriver, QueryQueue, QueryQueueOptions } from '../../src';

export type QueryQueueTestOptions = Pick<QueryQueueOptions, 'cacheAndQueueDriver' | 'cubeStoreDriverFactory'> & {
  beforeAll?: () => Promise<void>,
  afterAll?: () => Promise<void>,
  workers?: number,
};

function patchQueueDriverConnectionForTrack(connection: QueueDriverConnectionInterface, counters: any): QueueDriverConnectionInterface {
  function wrapAsyncMethod<M extends MethodName<QueueDriverConnectionInterface>>(methodName: M): any {
    return async (...args: Parameters<QueueDriverConnectionInterface[M]>) => {
      if (!(methodName in counters.methods)) {
        counters.methods[methodName] = {
          started: 1,
          finished: 0,
        };
      } else {
        counters.methods[methodName].started++;
      }

      const result = await (connection[methodName] as any)(...args);
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

export function QueryQueueBenchmark(name: string, options: QueryQueueTestOptions) {
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
          stream: async (_query, _stream) => {
            throw new Error('streaming handler is not supported for testing');
          }
        },
        cancelHandlers: {
          query: async (_query) => {
            console.error('Cancel handler was called for query');
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

          if (event.includes('error')) {
            console.log(event, _params);
          }
        },
        queueDriverFactory,
        ...options
      });

      // Spawn worker processes for multi-process simulation (CubeStore only)
      type WorkerCounters = { handlersStarted: number; handlersFinished: number; events: Record<string, number> };
      type WorkerState = { worker: ChildProcess; counters: WorkerCounters; prevFinished: number };
      const workerStates: WorkerState[] = [];
      const numWorkers = options.workers || 0;

      if (numWorkers > 0) {
        const workerPath = path.resolve(__dirname, 'QueueBenchWorker.js');

        for (let i = 0; i < numWorkers; i++) {
          const w = fork(workerPath, [], {
            execArgv: process.execArgv,
            stdio: ['inherit', 'inherit', 'inherit', 'ipc'],
          });

          const state: WorkerState = {
            worker: w,
            counters: { handlersStarted: 0, handlersFinished: 0, events: {} },
            prevFinished: 0,
          };

          w.on('message', (msg: { type: string; data?: WorkerCounters }) => {
            if (msg.type === 'counters' && msg.data) {
              state.counters = msg.data;
            }
          });

          w.on('error', (err) => {
            console.error(`[Worker ${i}] error:`, err);
          });

          w.send({
            type: 'start',
            tenantPrefix,
            benchSettings: {
              queueResponseSize: benchSettings.queueResponseSize,
              currency: benchSettings.currency,
            },
            reconcileInterval: 50,
          });

          workerStates.push(state);
        }

        console.log(`Spawned ${numWorkers} worker processes`);
      }

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

        // Shutdown worker processes
        if (workerStates.length > 0) {
          await Promise.all(workerStates.map((ws) => new Promise<void>((resolve) => {
            const onMessage = (msg: { type: string; data?: WorkerCounters }) => {
              if (msg.type === 'counters' && msg.data) {
                ws.counters = msg.data;
              }
              if (msg.type === 'done') {
                ws.worker.removeListener('message', onMessage);
                resolve();
              }
            };

            ws.worker.on('message', onMessage);
            ws.worker.send({ type: 'shutdown' });
          })));
        }
      }

      const progressIntervalId = setInterval(() => {
        console.log('running', {
          ...counters,
          processingPromisses: processingPromisses.length,
          benchSettings,
          ...(workerStates.length > 0 ? {
            workers: workerStates.map((ws, i) => {
              const finishedSinceLastTick = ws.counters.handlersFinished - ws.prevFinished;
              ws.prevFinished = ws.counters.handlersFinished;
              return {
                id: i,
                handlersStarted: ws.counters.handlersStarted,
                handlersFinished: ws.counters.handlersFinished,
                processing: ws.counters.handlersStarted - ws.counters.handlersFinished,
                processedFromLastEvent: finishedSinceLastTick,
              };
            }),
          } : {}),
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
          try {
            await queue.executeInQueue('query', queueId, {
              // eslint-disable-next-line no-bitwise
              payload: {
                large_str: 'a'.repeat(benchSettings.queuePayloadSize)
              },
              orphanedTimeout: 120
            }, 1, {
              stageQueryKey: 1,
              requestId: 'request-id',
              spanId: 'span-id'
            });
          } catch (e) {
            console.error(e);
          }

          counters.queueResolved++;

          // losing memory for a result
          return null;
        })();

        processingPromisses.push(running);
        await running;
      }, 10);

      await lock.promise;
      await awaitProcessing();
      clearInterval(progressIntervalId);

      const workerAgg = workerStates.reduce(
        (acc, ws) => ({
          handlersStarted: acc.handlersStarted + ws.counters.handlersStarted,
          handlersFinished: acc.handlersFinished + ws.counters.handlersFinished,
        }),
        { handlersStarted: 0, handlersFinished: 0 }
      );

      console.dir({
        message: 'Result',
        benchSettings,
        ...counters,
        ...(workerStates.length > 0 ? {
          workers: workerStates.map((ws, i) => ({
            id: i,
            ...ws.counters,
            processing: ws.counters.handlersStarted - ws.counters.handlersFinished,
          })),
          totalHandlersStarted: counters.handlersStarted + workerAgg.handlersStarted,
          totalHandlersFinished: counters.handlersFinished + workerAgg.handlersFinished,
        } : {}),
      }, { depth: null });
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

    process.exit(0);
  })();
}
