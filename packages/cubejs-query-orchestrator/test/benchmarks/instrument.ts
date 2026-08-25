import { CubeStoreQueueDriver } from '@cubejs-backend/cubestore-driver';
import { MethodName } from '@cubejs-backend/shared';
import {
  AddToQueueResponse,
  QueueDriverConnectionInterface,
  QueueDriverInterface,
  QueuePriority,
} from '@cubejs-backend/base-driver';
import { LocalQueueDriver } from '../../src';

export type MethodCounter = { started: number, finished: number };

export type BenchCounters = {
  connections: number,
  methods: Record<string, MethodCounter>,
  events: Record<string, number>,
  handlersStarted: number,
  handlersFinished: number,
  fastTrack: { attempts: number, hits: number },
};

export function createCounters(): BenchCounters {
  return {
    connections: 0,
    methods: {},
    events: {},
    handlersStarted: 0,
    handlersFinished: 0,
    fastTrack: { attempts: 0, hits: 0 },
  };
}

export function driverCallsTotal(counters: Pick<BenchCounters, 'methods'>): number {
  return Object.values(counters.methods).reduce((acc, m) => acc + m.started, 0);
}

export function mergeMethods(into: Record<string, MethodCounter>, from: Record<string, MethodCounter>) {
  for (const [name, m] of Object.entries(from)) {
    if (name in into) {
      into[name].started += m.started;
      into[name].finished += m.finished;
    } else {
      into[name] = { started: m.started, finished: m.finished };
    }
  }

  return into;
}

export function mergeEvents(into: Record<string, number>, from: Record<string, number>) {
  for (const [name, count] of Object.entries(from)) {
    into[name] = (into[name] || 0) + count;
  }

  return into;
}

export type Percentiles = { count: number, mean: number, p50: number, p90: number, p95: number, p99: number, max: number };

export function percentiles(samples: number[]): Percentiles {
  if (samples.length === 0) {
    return { count: 0, mean: 0, p50: 0, p90: 0, p95: 0, p99: 0, max: 0 };
  }

  const sorted = [...samples].sort((a, b) => a - b);
  const at = (q: number) => Math.round(sorted[Math.min(sorted.length - 1, Math.max(0, Math.ceil(q * sorted.length) - 1))]);

  return {
    count: sorted.length,
    mean: Math.round(sorted.reduce((a, b) => a + b, 0) / sorted.length),
    p50: at(0.5),
    p90: at(0.9),
    p95: at(0.95),
    p99: at(0.99),
    max: Math.round(sorted[sorted.length - 1]),
  };
}

/**
 * Asking the connection itself, rather than reading the env, covers the version capability check
 * and the priority floor too, so a driver that cannot fast track never registers an attempt and
 * the miss rate stays a property of the queue rather than of the deployment.
 */
async function fastTrackEligible(connection: QueueDriverConnectionInterface, priority: QueuePriority): Promise<boolean> {
  const { useFastTrack } = connection as any;

  return typeof useFastTrack === 'function' ? useFastTrack.call(connection, priority) : false;
}

function patchQueueDriverConnectionForTrack(connection: QueueDriverConnectionInterface, counters: BenchCounters): QueueDriverConnectionInterface {
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

  const trackedAddToQueue = wrapAsyncMethod('addToQueue');

  const tracked: Record<string, any> = {
    addToQueue: async (...args: Parameters<QueueDriverConnectionInterface['addToQueue']>) => {
      const eligible = await fastTrackEligible(connection, args[3]);
      if (eligible) {
        counters.fastTrack.attempts++;
      }

      const result: AddToQueueResponse = await trackedAddToQueue(...args);
      if (eligible && result[4]) {
        counters.fastTrack.hits++;
      }

      return result;
    },
    getResult: wrapAsyncMethod('getResult'),
    getQueriesToCancel: wrapAsyncMethod('getQueriesToCancel'),
    getActiveAndToProcess: wrapAsyncMethod('getActiveAndToProcess'),
    retrieveForProcessing: wrapAsyncMethod('retrieveForProcessing'),
    getQueryDef: wrapAsyncMethod('getQueryDef'),
    setResultAndRemoveQuery: wrapAsyncMethod('setResultAndRemoveQuery'),
    getQueryStageState: wrapAsyncMethod('getQueryStageState'),
    getResultBlocking: wrapAsyncMethod('getResultBlocking'),
    optimisticQueryUpdate: wrapAsyncMethod('optimisticQueryUpdate'),
    getQueryAndRemove: wrapAsyncMethod('getQueryAndRemove'),
  };

  // Spreading the connection would copy own properties only and silently drop every method that
  // lives on the prototype and is not re-listed above — `updateHeartBeat` among them, which
  // QueryQueue calls on a timer during a long handler
  return new Proxy(connection, {
    get: (target, prop) => {
      if (typeof prop === 'string' && prop in tracked) {
        return tracked[prop];
      }

      const value = Reflect.get(target, prop, target);

      return typeof value === 'function' ? value.bind(target) : value;
    },
  });
}

export function patchQueueDriverForTrack(driver: QueueDriverInterface, counters: BenchCounters): QueueDriverInterface {
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

/**
 * Both the main process and the workers count through the same wrapper, otherwise every run with
 * `workers > 0` reports the driver calls of the main process only
 */
export function makeQueueDriverFactory(counters: BenchCounters, cubeStoreDriverFactory?: () => Promise<any>) {
  return (driverType: string, queueDriverOptions: any) => {
    switch (driverType) {
      case 'memory':
        return patchQueueDriverForTrack(
          new LocalQueueDriver(queueDriverOptions) as any,
          counters
        );
      case 'cubestore':
        return patchQueueDriverForTrack(
          new CubeStoreQueueDriver(
            async () => cubeStoreDriverFactory(),
            queueDriverOptions
          ),
          counters
        );
      default:
        throw new Error(`Unsupported driver: ${driverType}`);
    }
  };
}
