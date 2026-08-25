// eslint-disable-next-line import/no-extraneous-dependencies
import 'source-map-support/register';

import { CubeStoreDriver } from '@cubejs-backend/cubestore-driver';
import { pausePromise } from '@cubejs-backend/shared';
import { QueryQueue } from '../../src';
import { createCounters, makeQueueDriverFactory } from './instrument';
import { WorkerMessage, WorkerSnapshot, WorkerStartMessage } from './protocol';

if (!process.send) {
  throw new Error('QueueBenchWorker must be run as a child process with IPC');
}

// A killed run must not leave a worker polling Cube Store forever
process.on('disconnect', () => process.exit(0));

const counters = createCounters();

let cubeStoreDriver: CubeStoreDriver;
let queue: QueryQueue;
let reconcileId: ReturnType<typeof setInterval>;

function snapshot(): WorkerSnapshot {
  return {
    handlersStarted: counters.handlersStarted,
    handlersFinished: counters.handlersFinished,
    methods: JSON.parse(JSON.stringify(counters.methods)),
    events: { ...counters.events },
    fastTrack: { ...counters.fastTrack },
  };
}

process.on('message', async (msg: WorkerMessage) => {
  if (msg.type === 'start') {
    const { tenantPrefix, benchSettings, reconcileIntervalMs } = msg as WorkerStartMessage;

    cubeStoreDriver = new CubeStoreDriver({});

    queue = new QueryQueue(`${tenantPrefix}#test_query_queue`, {
      queryHandlers: {
        query: async () => {
          counters.handlersStarted++;
          await pausePromise(benchSettings.handlerLatencyMs);
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
      concurrency: benchSettings.concurrency,
      cacheAndQueueDriver: 'cubestore',
      queueDriverFactory: makeQueueDriverFactory(counters, async () => cubeStoreDriver),
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

    // A worker never submits, so it has no submit-time reconcile to bootstrap from. This poll is
    // an artifact of the harness — production reconcile is event-driven — which is why its
    // interval is a setting and lands in the reported run settings.
    reconcileId = setInterval(() => {
      queue.reconcileQueue();
    }, reconcileIntervalMs);
  }

  // Answering on request instead of pushing on a timer keeps the worker numbers inside the tick
  // they belong to
  if (msg.type === 'tickRequest') {
    process.send!({ type: 'counters', seq: msg.seq, data: snapshot() });
  }

  if (msg.type === 'shutdown') {
    clearInterval(reconcileId);

    await queue.shutdown();
    await cubeStoreDriver.release();

    process.send!({ type: 'counters', seq: -1, data: snapshot() });
    process.send!({ type: 'done' });
    process.disconnect();
    process.exit(0);
  }
});
