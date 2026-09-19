// eslint-disable-next-line import/no-extraneous-dependencies
import 'source-map-support/register';

import { CubeStoreDriver } from '@cubejs-backend/cubestore-driver';
import { QueryQueue } from '../../src';
import { countEvent, createBenchQueue, createCounters } from './instrument';
import { counterSnapshot, WorkerMessage, WorkerStartMessage } from './protocol';

if (!process.send) {
  throw new Error('QueueBenchWorker must be run as a child process with IPC');
}

// A killed run must not leave a worker polling Cube Store forever
process.on('disconnect', () => process.exit(0));

const RECONCILE_ERROR_EVENT = 'bench reconcile poll error';

const counters = createCounters();

let cubeStoreDriver: CubeStoreDriver;
let queue: QueryQueue;
let reconcileId: ReturnType<typeof setInterval>;

process.on('message', async (msg: WorkerMessage) => {
  if (msg.type === 'start') {
    const { tenantPrefix, benchSettings, reconcileIntervalMs } = msg as WorkerStartMessage;

    cubeStoreDriver = new CubeStoreDriver({});

    queue = createBenchQueue(
      `${tenantPrefix}#test_query_queue`,
      counters,
      benchSettings,
      {
        cacheAndQueueDriver: 'cubestore',
        cubeStoreDriverFactory: async () => cubeStoreDriver,
      },
      '[Worker] ',
    );

    // A worker never submits, so it has no submit-time reconcile to bootstrap from. This poll is
    // an artifact of the harness — production reconcile is event-driven — which is why its
    // interval is a setting and lands in the reported run settings.
    reconcileId = setInterval(() => {
      // reconcileQueue rethrows, and an unhandled rejection takes the process down under
      // Node's default policy — the run would then keep reporting as if the worker were here
      queue.reconcileQueue().catch((e) => {
        countEvent(counters, RECONCILE_ERROR_EVENT);
        console.error('[Worker]', RECONCILE_ERROR_EVENT, e);
      });
    }, reconcileIntervalMs);
  }

  // Answering on request instead of pushing on a timer keeps the worker numbers inside the tick
  // they belong to
  if (msg.type === 'tickRequest') {
    process.send!({ type: 'counters', seq: msg.seq, data: counterSnapshot(counters) });
  }

  if (msg.type === 'shutdown') {
    clearInterval(reconcileId);

    await queue.shutdown();
    await cubeStoreDriver.release();

    process.send!({ type: 'counters', seq: -1, data: counterSnapshot(counters) });
    process.send!({ type: 'done' });
    process.disconnect();
    process.exit(0);
  }
});
