import crypto from 'crypto';
import path from 'path';
import { ChildProcess, fork } from 'child_process';
import { createPromiseLock, pausePromise } from '@cubejs-backend/shared';
import { QueuePriority } from '@cubejs-backend/base-driver';
import { ContinueWaitError, QueryQueue, QueryQueueOptions, TimeoutError } from '../../src';
import {
  BenchCounters,
  createCounters,
  driverCallsTotal,
  makeQueueDriverFactory,
  MethodCounter,
  mergeEvents,
  mergeMethods,
  percentiles,
} from './instrument';
import { ParentMessage, WorkerSnapshot } from './protocol';

export type QueryQueueTestOptions = Pick<QueryQueueOptions, 'cacheAndQueueDriver' | 'cubeStoreDriverFactory'> & {
  beforeAll?: () => Promise<void>,
  afterAll?: () => Promise<void>,
  workers?: number,
};

type PriorityBucket = { priority: number, weight: number };

type BenchSettings = {
  driver: string,
  fastTrack: boolean,
  workers: number,
  concurrency: number,
  totalQueries: number,
  periodMs: number,
  pushIntervalMs: number,
  priority: QueuePriority,
  priorityMix: PriorityBucket[] | null,
  handlerLatencyMs: number,
  queueResponseSize: number,
  queuePayloadSize: number,
  workerReconcileMs: number,
  warmupQueries: number,
  idleTailMs: number,
  tickMs: number,
};

type Phase = 'warmup' | 'measure' | 'drain' | 'idle';

type Aggregate = {
  methods: Record<string, MethodCounter>,
  events: Record<string, number>,
  handlersStarted: number,
  handlersFinished: number,
  fastTrack: { attempts: number, hits: number },
  driverCalls: number,
};

const EMPTY_AGGREGATE = (): Aggregate => ({
  methods: {},
  events: {},
  handlersStarted: 0,
  handlersFinished: 0,
  fastTrack: { attempts: 0, hits: 0 },
  driverCalls: 0,
});

function toAggregate(source: Pick<BenchCounters, 'methods' | 'events' | 'handlersStarted' | 'handlersFinished' | 'fastTrack'>): Aggregate {
  return {
    methods: JSON.parse(JSON.stringify(source.methods)),
    events: { ...source.events },
    handlersStarted: source.handlersStarted,
    handlersFinished: source.handlersFinished,
    fastTrack: { ...source.fastTrack },
    driverCalls: driverCallsTotal(source),
  };
}

function addAggregate(into: Aggregate, from: Aggregate): Aggregate {
  mergeMethods(into.methods, from.methods);
  mergeEvents(into.events, from.events);
  into.handlersStarted += from.handlersStarted;
  into.handlersFinished += from.handlersFinished;
  into.fastTrack.attempts += from.fastTrack.attempts;
  into.fastTrack.hits += from.fastTrack.hits;
  into.driverCalls += from.driverCalls;

  return into;
}

/**
 * Warmup is charged against a baseline instead of a counter reset, so the workers need no reset
 * round trip and the subtraction is identical on every source
 */
function subAggregate(a: Aggregate, b: Aggregate): Aggregate {
  const methods: Record<string, MethodCounter> = {};
  for (const [name, m] of Object.entries(a.methods)) {
    const base = b.methods[name] || { started: 0, finished: 0 };
    methods[name] = { started: m.started - base.started, finished: m.finished - base.finished };
  }

  const events: Record<string, number> = {};
  for (const [name, count] of Object.entries(a.events)) {
    events[name] = count - (b.events[name] || 0);
  }

  return {
    methods,
    events,
    handlersStarted: a.handlersStarted - b.handlersStarted,
    handlersFinished: a.handlersFinished - b.handlersFinished,
    fastTrack: {
      attempts: a.fastTrack.attempts - b.fastTrack.attempts,
      hits: a.fastTrack.hits - b.fastTrack.hits,
    },
    driverCalls: a.driverCalls - b.driverCalls,
  };
}

function parsePriorityMix(raw: string | undefined): PriorityBucket[] | null {
  if (!raw) {
    return null;
  }

  const buckets = raw.split(',').map((part) => {
    const [priority, weight] = part.split(':');
    return { priority: parseInt(priority, 10), weight: parseInt(weight, 10) };
  });

  if (buckets.some((b) => Number.isNaN(b.priority) || Number.isNaN(b.weight) || b.weight <= 0)) {
    throw new Error(`Malformed BENCH_PRIORITY_MIX: ${raw}, expected "10:50,0:50"`);
  }

  return buckets;
}

/**
 * Largest-remainder apportionment, so an uneven mix still interleaves instead of arriving in blocks
 */
function pickBucket(buckets: PriorityBucket[], assigned: number[], index: number): number {
  const total = buckets.reduce((acc, b) => acc + b.weight, 0);
  let best = 0;
  let bestScore = -Infinity;

  for (let i = 0; i < buckets.length; i++) {
    const score = (buckets[i].weight * (index + 1)) / total - assigned[i];
    if (score > bestScore) {
      bestScore = score;
      best = i;
    }
  }

  return best;
}

function readSettings(driver: string, workers: number): BenchSettings {
  const totalQueries = parseInt(process.env.BENCH_TOTAL_QUERIES || '1000', 10);
  // BENCH_PERIOD_MS spreads the queries evenly over that window instead of pushing them
  // as fast as the loop allows, which is what decides whether the queue ever backlogs
  const periodMs = parseInt(process.env.BENCH_PERIOD_MS || '0', 10);

  return {
    driver,
    fastTrack: process.env.CUBEJS_QUEUE_FAST_TRACK === 'true',
    workers,
    concurrency: parseInt(process.env.BENCH_CONCURRENCY || '50', 10),
    totalQueries,
    periodMs,
    pushIntervalMs: periodMs > 0 && totalQueries > 0 ? Math.max(1, Math.round(periodMs / totalQueries)) : 10,
    priority: parseInt(process.env.BENCH_PRIORITY || `${QueuePriority.Interactive}`, 10),
    priorityMix: parsePriorityMix(process.env.BENCH_PRIORITY_MIX),
    handlerLatencyMs: parseInt(process.env.BENCH_HANDLER_LATENCY_MS || '1500', 10),
    // eslint-disable-next-line no-bitwise
    queueResponseSize: parseInt(process.env.BENCH_RESPONSE_SIZE || `${5 << 20}`, 10),
    queuePayloadSize: parseInt(process.env.BENCH_PAYLOAD_SIZE || `${256 * 1024}`, 10),
    workerReconcileMs: parseInt(process.env.BENCH_WORKER_RECONCILE_MS || '50', 10),
    warmupQueries: parseInt(process.env.BENCH_WARMUP_QUERIES || '0', 10),
    idleTailMs: parseInt(process.env.BENCH_IDLE_TAIL_MS || '0', 10),
    tickMs: parseInt(process.env.BENCH_TICK_MS || '1000', 10),
  };
}

export function QueryQueueBenchmark(name: string, options: QueryQueueTestOptions) {
  (async () => {
    if (options.beforeAll) {
      await options.beforeAll();
    }

    const createBenchmark = async (benchSettings: BenchSettings) => {
      const counters = createCounters();

      const tenantPrefix = crypto.randomBytes(6).toString('hex');
      const queue = new QueryQueue(`${tenantPrefix}#test_query_queue`, {
        queryHandlers: {
          query: async (_query) => {
            counters.handlersStarted++;
            await pausePromise(benchSettings.handlerLatencyMs);
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
        concurrency: benchSettings.concurrency,
        logger: (event, _params) => {
          if (event in counters.events) {
            counters.events[event]++;
          } else {
            counters.events[event] = 1;
          }

          if (event.includes('error')) {
            console.log(event, _params);
          }
        },
        queueDriverFactory: makeQueueDriverFactory(counters, options.cubeStoreDriverFactory),
        cacheAndQueueDriver: options.cacheAndQueueDriver,
        cubeStoreDriverFactory: options.cubeStoreDriverFactory,
      });

      type WorkerState = {
        worker: ChildProcess,
        latest: WorkerSnapshot,
        baseline: Aggregate,
        awaiting: { seq: number, resolve: () => void } | null,
        alive: boolean,
      };
      const workerStates: WorkerState[] = [];
      const numWorkers = options.workers || 0;
      const shutdown = { requested: false };

      const emptySnapshot = (): WorkerSnapshot => ({
        handlersStarted: 0,
        handlersFinished: 0,
        methods: {},
        events: {},
        fastTrack: { attempts: 0, hits: 0 },
      });

      if (numWorkers > 0) {
        const workerPath = path.resolve(__dirname, 'QueueBenchWorker.js');

        for (let i = 0; i < numWorkers; i++) {
          const w = fork(workerPath, [], {
            execArgv: process.execArgv,
            stdio: ['inherit', 'inherit', 'inherit', 'ipc'],
          });

          const state: WorkerState = {
            worker: w,
            latest: emptySnapshot(),
            baseline: EMPTY_AGGREGATE(),
            awaiting: null,
            alive: true,
          };

          w.on('message', (msg: ParentMessage) => {
            if (msg.type === 'counters') {
              state.latest = msg.data;

              if (state.awaiting && (state.awaiting.seq === msg.seq || msg.seq === -1)) {
                state.awaiting.resolve();
                state.awaiting = null;
              }
            }
          });

          w.on('error', (err) => {
            console.error(`[Worker ${i}] error:`, err);
          });

          // Without this a dead worker keeps its last snapshot and every tick waits the full
          // timeout for a reply that cannot come, while the run reports as if it were still there
          w.on('exit', (code, signal) => {
            if (!shutdown.requested) {
              console.error(`[Worker ${i}] exited early with ${signal || `code ${code}`} — its counters stop here`);
            }

            state.alive = false;
            state.awaiting?.resolve();
            state.awaiting = null;
          });

          w.send({
            type: 'start',
            tenantPrefix,
            benchSettings: {
              queueResponseSize: benchSettings.queueResponseSize,
              concurrency: benchSettings.concurrency,
              handlerLatencyMs: benchSettings.handlerLatencyMs,
            },
            reconcileIntervalMs: benchSettings.workerReconcileMs,
          });

          workerStates.push(state);
        }

        console.log(`Spawned ${numWorkers} worker processes`);
      }

      let tickSeq = 0;

      async function collectWorkerSnapshots(timeoutMs = 200): Promise<number> {
        if (workerStates.length === 0) {
          return 0;
        }

        const seq = ++tickSeq;
        const requestedAt = Date.now();

        const waits = workerStates.filter((ws) => ws.alive).map((ws) => new Promise<void>((resolve) => {
          if (ws.awaiting) {
            ws.awaiting.resolve();
          }

          if (!ws.worker.connected) {
            ws.alive = false;
            resolve();

            return;
          }

          ws.awaiting = { seq, resolve };
          // Never reject: this promise loses the race below whenever a worker is slow, and a
          // rejection settled after the loser is dropped would surface as an unhandled rejection
          ws.worker.send({ type: 'tickRequest', seq }, (err) => {
            if (err) {
              ws.awaiting = null;
              resolve();
            }
          });
        }));

        if (waits.length === 0) {
          return 0;
        }

        await Promise.race([Promise.all(waits), pausePromise(timeoutMs)]);

        return Date.now() - requestedAt;
      }

      function snapshotAggregate(): Aggregate {
        const total = toAggregate(counters);
        for (const ws of workerStates) {
          addAggregate(total, toAggregate(ws.latest));
        }

        return total;
      }

      const runStartedAt = Date.now();
      let phase: Phase = benchSettings.warmupQueries > 0 ? 'warmup' : 'measure';
      let measureStartedAt = runStartedAt;
      let baseline = EMPTY_AGGREGATE();
      let baselineMain = EMPTY_AGGREGATE();

      let pushed = 0;
      let completed = 0;
      const failed = { continueWait: 0, timeout: 0, other: 0 };
      const errorSamples: string[] = [];
      let latencies: number[] = [];
      let latenciesByPriority: Record<number, number[]> = {};

      const inFlight = () => pushed - completed - failed.continueWait - failed.timeout - failed.other;

      const processingPromisses: Promise<null>[] = [];

      const bucketAssigned = benchSettings.priorityMix ? benchSettings.priorityMix.map(() => 0) : [];

      function priorityFor(index: number): QueuePriority {
        if (!benchSettings.priorityMix) {
          return benchSettings.priority;
        }

        const bucket = pickBucket(benchSettings.priorityMix, bucketAssigned, index);
        bucketAssigned[bucket]++;

        return benchSettings.priorityMix[bucket].priority;
      }

      function pushOne(index: number) {
        pushed++;

        const priority = priorityFor(index);
        const queueId = crypto.randomBytes(12).toString('hex');
        const startedAt = process.hrtime.bigint();

        const running = (async () => {
          try {
            await queue.executeInQueue('query', queueId, {
              payload: {
                large_str: 'a'.repeat(benchSettings.queuePayloadSize)
              },
              orphanedTimeout: 120
            }, priority, {
              stageQueryKey: 1,
              requestId: 'request-id',
              spanId: 'span-id'
            });

            completed++;

            const latencyMs = Number(process.hrtime.bigint() - startedAt) / 1e6;
            latencies.push(latencyMs);
            (latenciesByPriority[priority] ||= []).push(latencyMs);
          } catch (e: any) {
            if (e instanceof ContinueWaitError) {
              failed.continueWait++;
            } else if (e instanceof TimeoutError) {
              failed.timeout++;
            } else {
              failed.other++;
            }

            if (errorSamples.length < 3) {
              errorSamples.push(`${e?.constructor?.name}: ${e?.message}`);
            }
          }

          // The result is dropped rather than returned, so 1000 payloads are not held alive
          // by the promise array until the run ends
          return null;
        })();

        processingPromisses.push(running);
      }

      function runPusher(count: number, intervalMs: number): Promise<void> {
        if (count <= 0) {
          return Promise.resolve();
        }

        const lock = createPromiseLock();
        let index = 0;

        const pusherIntervalId = setInterval(() => {
          if (index >= count) {
            clearInterval(pusherIntervalId);
            lock.resolve();

            return;
          }

          pushOne(index);
          index++;
        }, intervalMs);

        return lock.promise as Promise<void>;
      }

      async function drain() {
        // process query can call reconcileQueue
        while (await queue.shutdown() || processingPromisses.length) {
          await Promise.all(processingPromisses.splice(0));
        }
      }

      let prevDriverCalls = 0;
      let prevHandlersFinished = 0;
      let prevTickAt = runStartedAt;
      let peakDriverCallsPerSec = 0;
      let ticking = false;

      const tickIntervalId = setInterval(async () => {
        if (ticking) {
          return;
        }
        ticking = true;

        try {
          const workerSnapshotAgeMs = await collectWorkerSnapshots();
          const now = Date.now();
          const agg = subAggregate(snapshotAggregate(), baseline);

          const driverCallsDelta = agg.driverCalls - prevDriverCalls;
          const elapsedSinceTick = Math.max(1, now - prevTickAt);
          const perSec = (driverCallsDelta * 1000) / elapsedSinceTick;

          if (phase === 'measure' || phase === 'drain') {
            peakDriverCallsPerSec = Math.max(peakDriverCallsPerSec, perSec);
          }

          console.log(`BENCH_TICK ${JSON.stringify({
            runId: process.env.BENCH_RUN_ID || null,
            tMs: now - runStartedAt,
            phase,
            pushed,
            inFlight: inFlight(),
            completed,
            failed: failed.continueWait + failed.timeout + failed.other,
            driverCallsTotal: agg.driverCalls,
            driverCallsDelta,
            driverCallsPerSec: Math.round(perSec),
            handlersFinished: agg.handlersFinished,
            handlersFinishedDelta: agg.handlersFinished - prevHandlersFinished,
            fastTrackAttempts: agg.fastTrack.attempts,
            fastTrackHits: agg.fastTrack.hits,
            workerSnapshotAgeMs,
          })}`);

          prevDriverCalls = agg.driverCalls;
          prevHandlersFinished = agg.handlersFinished;
          prevTickAt = now;
        } finally {
          ticking = false;
        }
      }, benchSettings.tickMs);

      if (benchSettings.warmupQueries > 0) {
        await runPusher(benchSettings.warmupQueries, Math.min(benchSettings.pushIntervalMs, 50));
        await drain();

        await collectWorkerSnapshots();
        baseline = snapshotAggregate();
        baselineMain = toAggregate(counters);
        for (const ws of workerStates) {
          ws.baseline = toAggregate(ws.latest);
        }

        pushed = 0;
        completed = 0;
        failed.continueWait = 0;
        failed.timeout = 0;
        failed.other = 0;
        latencies = [];
        latenciesByPriority = {};
        errorSamples.splice(0);
        prevDriverCalls = 0;
        prevHandlersFinished = 0;
      }

      phase = 'measure';
      measureStartedAt = Date.now();

      await runPusher(benchSettings.totalQueries, benchSettings.pushIntervalMs);
      const pushEndedAt = Date.now();

      phase = 'drain';
      await drain();
      const drainEndedAt = Date.now();

      // Without a fresh pull the idle delta absorbs up to a tick of worker polling that predates it
      await collectWorkerSnapshots();
      const aggBeforeIdle = subAggregate(snapshotAggregate(), baseline);

      if (benchSettings.idleTailMs > 0) {
        phase = 'idle';
        await pausePromise(benchSettings.idleTailMs);
      }

      clearInterval(tickIntervalId);
      await collectWorkerSnapshots();

      const finalAgg = subAggregate(snapshotAggregate(), baseline);
      const idleDriverCalls = benchSettings.idleTailMs > 0 ? finalAgg.driverCalls - aggBeforeIdle.driverCalls : 0;

      shutdown.requested = true;

      if (workerStates.length > 0) {
        await Promise.all(workerStates.map((ws) => new Promise<void>((resolve) => {
          if (!ws.alive || !ws.worker.connected) {
            resolve();

            return;
          }

          const onMessage = (msg: ParentMessage) => {
            if (msg.type === 'counters') {
              ws.latest = msg.data;
            }
            if (msg.type === 'done') {
              ws.worker.removeListener('message', onMessage);
              resolve();
            }
          };

          ws.worker.on('message', onMessage);
          // A worker that dies before answering would otherwise hold this promise open forever
          ws.worker.once('exit', resolve);
          ws.worker.send({ type: 'shutdown' }, (err) => {
            if (err) {
              resolve();
            }
          });
        })));
      }

      const pushWindowMs = pushEndedAt - measureStartedAt;
      const capacityQps = (benchSettings.concurrency * 1000) / benchSettings.handlerLatencyMs;
      const targetRateQps = benchSettings.periodMs > 0
        ? (benchSettings.totalQueries * 1000) / benchSettings.periodMs
        : null;
      const actualRateQps = pushWindowMs > 0 ? (pushed * 1000) / pushWindowMs : null;
      const processes = benchSettings.workers + 1;

      const round = (v: number | null, digits = 3) => (v === null ? null : Number(v.toFixed(digits)));

      const result = {
        runId: process.env.BENCH_RUN_ID || null,
        suite: process.env.BENCH_SUITE || null,
        label: process.env.BENCH_LABEL || null,
        axis: process.env.BENCH_AXIS ? JSON.parse(process.env.BENCH_AXIS) : null,
        settings: benchSettings,
        derived: {
          capacityQps: round(capacityQps),
          targetRateQps: round(targetRateQps),
          actualRateQps: round(actualRateQps),
          targetRho: round(targetRateQps === null ? null : targetRateQps / capacityQps),
          actualRho: round(actualRateQps === null ? null : actualRateQps / capacityQps),
        },
        timing: {
          startedAt: new Date(runStartedAt).toISOString(),
          measureStartedAtMs: measureStartedAt - runStartedAt,
          pushWindowMs,
          drainMs: drainEndedAt - pushEndedAt,
          elapsedMs: drainEndedAt - measureStartedAt,
          idleTailMs: benchSettings.idleTailMs,
        },
        outcome: {
          pushed,
          completed,
          inFlightAtEnd: inFlight(),
          failed: { ...failed, total: failed.continueWait + failed.timeout + failed.other },
          errorSamples,
        },
        latencyMs: percentiles(latencies),
        latencyMsByPriority: Object.fromEntries(
          Object.entries(latenciesByPriority).map(([priority, samples]) => [priority, percentiles(samples)])
        ),
        driverCalls: {
          total: finalAgg.driverCalls,
          perQuery: completed > 0 ? round(finalAgg.driverCalls / completed) : null,
          peakPerSec: Math.round(peakDriverCallsPerSec),
          byMethod: finalAgg.methods,
          main: subAggregate(toAggregate(counters), baselineMain).methods,
          workers: workerStates.map((ws) => subAggregate(toAggregate(ws.latest), ws.baseline).methods),
          fastTrack: {
            ...finalAgg.fastTrack,
            missRate: finalAgg.fastTrack.attempts > 0
              ? round(1 - finalAgg.fastTrack.hits / finalAgg.fastTrack.attempts)
              : null,
          },
        },
        events: {
          merged: finalAgg.events,
          main: subAggregate(toAggregate(counters), baselineMain).events,
          workers: workerStates.map((ws) => subAggregate(toAggregate(ws.latest), ws.baseline).events),
        },
        handlers: {
          started: finalAgg.handlersStarted,
          finished: finalAgg.handlersFinished,
          main: counters.handlersFinished - baselineMain.handlersFinished,
          workers: workerStates.map((ws) => ws.latest.handlersFinished - ws.baseline.handlersFinished),
        },
        idle: {
          driverCalls: idleDriverCalls,
          callsPerSecPerProcess: benchSettings.idleTailMs > 0
            ? round((idleDriverCalls * 1000) / benchSettings.idleTailMs / processes)
            : null,
        },
        connections: counters.connections,
      };

      console.log(`BENCH_RESULT ${JSON.stringify(result)}`);

      if (!process.env.BENCH_RUN_ID) {
        console.dir({ message: 'Result', ...result }, { depth: null });
      }
    };

    await createBenchmark(readSettings(name, options.workers || 0));

    if (options.afterAll) {
      await options.afterAll();
    }

    process.exit(0);
  })();
}
