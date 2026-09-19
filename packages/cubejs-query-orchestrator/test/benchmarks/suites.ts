import { QueuePriority } from '@cubejs-backend/base-driver';

export type BenchRun = {
  label: string,
  axis: Record<string, number | string>,
  env: Record<string, string>,
};

export type Suite = {
  name: string,
  description: string,
  driver: 'cubestore' | 'memory',
  runs: BenchRun[],
};

/**
 * The published fast track numbers were all taken at reduced payloads and that was never written
 * down anywhere. These are the same reduced values, stated once; S7 owns the payload axis.
 */
const DEFAULTS: Record<string, string> = {
  WORKERS: '2',
  BENCH_CONCURRENCY: '10',
  BENCH_HANDLER_LATENCY_MS: '1500',
  BENCH_RESPONSE_SIZE: `${64 * 1024}`,
  BENCH_PAYLOAD_SIZE: `${16 * 1024}`,
  BENCH_PRIORITY: `${QueuePriority.Interactive}`,
  BENCH_WORKER_RECONCILE_MS: '50',
  BENCH_WARMUP_QUERIES: '20',
  BENCH_IDLE_TAIL_MS: '0',
  BENCH_TICK_MS: '1000',
};

const num = (env: Record<string, string>, key: string) => parseInt(env[key] ?? DEFAULTS[key], 10);

function run(label: string, axis: Record<string, number | string>, env: Record<string, string>): BenchRun {
  return { label, axis, env: { ...DEFAULTS, ...env } };
}

export function capacityQps(concurrency: number, handlerLatencyMs: number): number {
  return (concurrency * 1000) / handlerLatencyMs;
}

function periodForRho(totalQueries: number, rho: number, concurrency: number, handlerLatencyMs: number): number {
  return Math.round((totalQueries * 1000) / (rho * capacityQps(concurrency, handlerLatencyMs)));
}

export function rhoOf(env: Record<string, string>): number | null {
  const periodMs = num(env, 'BENCH_PERIOD_MS');
  const total = num(env, 'BENCH_TOTAL_QUERIES');
  if (!periodMs || !total) {
    return null;
  }

  const rate = (total * 1000) / periodMs;

  return rate / capacityQps(num(env, 'BENCH_CONCURRENCY'), num(env, 'BENCH_HANDLER_LATENCY_MS'));
}

/** Wall clock a run cannot go below: the arrival window, or the time capacity needs to chew through it */
export function estimateRunMs(env: Record<string, string>): number {
  const total = num(env, 'BENCH_TOTAL_QUERIES');
  const capacity = capacityQps(num(env, 'BENCH_CONCURRENCY'), num(env, 'BENCH_HANDLER_LATENCY_MS'));
  const drainMs = total > 0 ? (total / capacity) * 1000 : 0;
  const warmupMs = num(env, 'BENCH_WARMUP_QUERIES') > 0
    ? (num(env, 'BENCH_WARMUP_QUERIES') / capacity) * 1000 + 2000
    : 0;

  return Math.round(Math.max(num(env, 'BENCH_PERIOD_MS') || 0, drainMs) + num(env, 'BENCH_IDLE_TAIL_MS') + warmupMs);
}

const S1: Suite = {
  name: 'S1',
  description: 'ρ-sweep — the main chart. Driver calls per completed query against the load factor, points clustered around ρ=1 where the knee is.',
  driver: 'cubestore',
  runs: [0.5, 0.75, 0.9, 1.0, 1.25, 1.75, 2.5].map((rho) => run(
    `rho=${rho}`,
    { rho },
    {
      BENCH_TOTAL_QUERIES: '1000',
      BENCH_PERIOD_MS: `${periodForRho(1000, rho, 10, 1500)}`,
    }
  )),
};

const S2: Suite = {
  name: 'S2',
  description: 'Fan-out at underload. The old report called the effect flat across processes, but measured it at ρ≈15 where the fast track degenerates.',
  driver: 'cubestore',
  runs: [0, 1, 2, 3, 4, 5].map((workers) => run(
    `workers=${workers}`,
    { workers, rho: 0.8 },
    {
      WORKERS: `${workers}`,
      BENCH_TOTAL_QUERIES: '500',
      BENCH_PERIOD_MS: `${periodForRho(500, 0.8, 10, 1500)}`,
    }
  )),
};

const S3: Suite = {
  name: 'S3',
  description: 'Burst then silence. The one shape where the fast track can be a net loss: an ADD_AND_RETRIEVE that comes back empty is a round trip spent for nothing.',
  driver: 'cubestore',
  runs: [50, 200].map((concurrency) => run(
    `burst-c${concurrency}`,
    { concurrency },
    {
      BENCH_CONCURRENCY: `${concurrency}`,
      BENCH_TOTAL_QUERIES: '1000',
      BENCH_PERIOD_MS: '5000',
      BENCH_IDLE_TAIL_MS: '60000',
    }
  )),
};

const S4: Suite = {
  name: 'S4',
  description: 'Priority mix, half Interactive half Background. Checks that background never fast-tracks and that it does not starve while interactive does.',
  driver: 'cubestore',
  runs: [0.8, 1.5].map((rho) => run(
    `mix-rho=${rho}`,
    { rho },
    {
      BENCH_PRIORITY_MIX: '10:50,0:50',
      BENCH_TOTAL_QUERIES: '1000',
      BENCH_PERIOD_MS: `${periodForRho(1000, rho, 10, 1500)}`,
    }
  )),
};

function idleRun(workers: number, reconcileMs: number): BenchRun {
  // Nothing polls without a worker, so the interval is not an axis of the control run
  const polled = workers > 0;

  return run(
    polled ? `idle-w${workers}-r${reconcileMs}` : `idle-w${workers}`,
    polled ? { workers, reconcileMs } : { workers },
    {
      WORKERS: `${workers}`,
      BENCH_TOTAL_QUERIES: '0',
      BENCH_PERIOD_MS: '0',
      BENCH_WARMUP_QUERIES: '0',
      BENCH_IDLE_TAIL_MS: '60000',
      BENCH_WORKER_RECONCILE_MS: `${reconcileMs}`,
    }
  );
}

const S5: Suite = {
  name: 'S5',
  description: 'Idle floor — driver calls per second per process on an empty queue. On idle the worker reconcile poll is the entire traffic, so both a harness-fast and a realistic interval are measured.',
  driver: 'cubestore',
  runs: [
    // Nothing polls here: the submitter has no timer of its own and there are no workers, so this
    // run is zero by construction. It is the control that says the rest of the table is the poll
    // and nothing else — and it carries no reconcile axis, because that setting only reaches workers
    idleRun(0, 50),
    ...[1, 2, 4].flatMap((workers) => [50, 1000].map((reconcileMs) => idleRun(workers, reconcileMs))),
  ],
};

const S6: Suite = {
  name: 'S6',
  description: 'Handler latency sweep at ρ=0.8. The shorter the handler, the larger the overhead share — the saving as a fraction of wall clock should peak at 100ms.',
  driver: 'cubestore',
  runs: [
    { latencyMs: 100, windowMs: 60000 },
    { latencyMs: 500, windowMs: 60000 },
    { latencyMs: 1500, windowMs: 60000 },
    // 1.6 q/s over a minute is too few samples for a p99, so this point gets a longer window
    { latencyMs: 5000, windowMs: 180000 },
  ].map(({ latencyMs, windowMs }) => {
    const total = Math.round(0.8 * capacityQps(10, latencyMs) * (windowMs / 1000));

    return run(
      `L=${latencyMs}ms`,
      { latencyMs, rho: 0.8 },
      {
        BENCH_HANDLER_LATENCY_MS: `${latencyMs}`,
        BENCH_TOTAL_QUERIES: `${total}`,
        BENCH_PERIOD_MS: `${windowMs}`,
      }
    );
  }),
};

const S7: Suite = {
  name: 'S7',
  description: 'Payload sweep. ADD_AND_RETRIEVE carries the query def inline, so on fat payloads the saved round trips can be eaten by the bytes.',
  driver: 'cubestore',
  runs: [
    // eslint-disable-next-line no-bitwise
    ...[0, 256 * 1024, 5 << 20, 20 << 20].map((responseSize) => run(
      `response=${responseSize}`,
      { responseSize, axis: 'response' },
      {
        BENCH_RESPONSE_SIZE: `${responseSize}`,
        BENCH_PAYLOAD_SIZE: `${16 * 1024}`,
        BENCH_TOTAL_QUERIES: '300',
        BENCH_PERIOD_MS: `${periodForRho(300, 0.8, 10, 1500)}`,
      }
    )),
    ...[0, 16 * 1024, 256 * 1024, 1024 * 1024].map((payloadSize) => run(
      `payload=${payloadSize}`,
      { payloadSize, axis: 'payload' },
      {
        BENCH_RESPONSE_SIZE: `${64 * 1024}`,
        BENCH_PAYLOAD_SIZE: `${payloadSize}`,
        BENCH_TOTAL_QUERIES: '300',
        BENCH_PERIOD_MS: `${periodForRho(300, 0.8, 10, 1500)}`,
      }
    )),
  ],
};

const S8: Suite = {
  name: 'S8',
  description: 'Crossing ρ=1 from the capacity side instead of the arrival side, at a fixed 8.33 q/s. Tests whether what decides is the budget or the ratio.',
  driver: 'cubestore',
  runs: [5, 10, 15, 20, 50].map((concurrency) => run(
    `c=${concurrency}`,
    { concurrency, rho: Number((8.3333 / capacityQps(concurrency, 1500)).toFixed(3)) },
    {
      BENCH_CONCURRENCY: `${concurrency}`,
      BENCH_TOTAL_QUERIES: '1000',
      BENCH_PERIOD_MS: '120000',
    }
  )),
};

const SMOKE: Suite = {
  name: 'smoke',
  description: 'Two-minute self-check on the memory driver — verifies ticks, percentiles and the result line without a Cube Store.',
  driver: 'memory',
  runs: [0.5, 1.5].map((rho) => run(
    `smoke-rho=${rho}`,
    { rho },
    {
      WORKERS: '0',
      BENCH_CONCURRENCY: '5',
      BENCH_HANDLER_LATENCY_MS: '100',
      BENCH_TOTAL_QUERIES: '200',
      BENCH_PERIOD_MS: `${periodForRho(200, rho, 5, 100)}`,
      BENCH_WARMUP_QUERIES: '10',
    }
  )),
};

export const SUITES: Suite[] = [S1, S2, S3, S4, S5, S6, S7, S8, SMOKE];

export function suiteByName(name: string): Suite {
  const suite = SUITES.find((s) => s.name.toLowerCase() === name.toLowerCase());
  if (!suite) {
    throw new Error(`Unknown suite: ${name}. Known: ${SUITES.map((s) => s.name).join(', ')}`);
  }

  return suite;
}
