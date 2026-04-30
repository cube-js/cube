// eslint-disable-next-line import/no-extraneous-dependencies
import 'source-map-support/register';

import { CubeStoreDriver } from '@cubejs-backend/cubestore-driver';
import { pausePromise } from '@cubejs-backend/shared';
import { QueryQueue } from '../../src';

if (!process.send) {
  throw new Error('QueueBenchWorker must be run as a child process with IPC');
}

const counters = {
  handlersStarted: 0,
  handlersFinished: 0,
  events: {} as Record<string, number>,
};

let cubeStoreDriver: CubeStoreDriver;
let queue: QueryQueue;
let reconcileId: ReturnType<typeof setInterval>;
let progressId: ReturnType<typeof setInterval>;

process.on('message', async (msg: { type: string; tenantPrefix?: string; benchSettings?: { queueResponseSize: number; currency: number; handlerLatencyMs?: number }; reconcileInterval?: number }) => {
  if (msg.type === 'start') {
    const { tenantPrefix, benchSettings, reconcileInterval } = msg as Required<typeof msg>;

    cubeStoreDriver = new CubeStoreDriver({});

    queue = new QueryQueue(`${tenantPrefix}#test_query_queue`, {
      queryHandlers: {
        query: async () => {
          counters.handlersStarted++;
          await pausePromise(benchSettings.handlerLatencyMs || 1500);
          counters.handlersFinished++;

          return {
            payload: 'a'.repeat(benchSettings.queueResponseSize),
          };
        },
        stream: async () => {
          throw new Error('streaming handler is not supported for testing');
        }
      },
      cancelHandlers: {
        query: async () => {
          console.error('[Worker] Cancel handler was called for query');
        },
      },
      continueWaitTimeout: 60 * 2,
      executionTimeout: 20,
      orphanedTimeout: 60 * 5,
      concurrency: benchSettings.currency,
      cacheAndQueueDriver: 'cubestore',
      cubeStoreDriverFactory: async () => cubeStoreDriver,
      logger: (event, _params) => {
        if (event in counters.events) {
          counters.events[event]++;
        } else {
          counters.events[event] = 1;
        }

        if (event.includes('error')) {
          console.log('[Worker]', event, _params);
        }
      },
    });

    // Periodically reconcile to pick up pending queries from CubeStore
    reconcileId = setInterval(() => {
      queue.reconcileQueue();
    }, reconcileInterval);

    // Report counters to main process periodically
    progressId = setInterval(() => {
      process.send!({
        type: 'counters',
        data: { ...counters, events: { ...counters.events } },
      });
    }, 1000);
  }

  if (msg.type === 'shutdown') {
    clearInterval(reconcileId);
    clearInterval(progressId);

    await queue.shutdown();
    await cubeStoreDriver.release();

    process.send!({
      type: 'counters',
      data: { ...counters, events: { ...counters.events } },
    });
    process.send!({ type: 'done' });
    process.disconnect();
    process.exit(0);
  }
});
