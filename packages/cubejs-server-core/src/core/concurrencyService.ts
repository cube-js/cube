import { getEnv } from '@cubejs-backend/shared';
import * as driverService from './driverService';
import { QueueOptions, RequestContext, OrchestratorOptions } from './types';
import type { CubejsServerCore } from './server';

/**
 * Wrap queueOptions into a function which evaluate concurrency on the fly.
 */
const queueOptionsWrapper = (
  context: RequestContext,
  queueOptions: unknown | ((dataSource?: string) => QueueOptions),
): (dataSource?: string
) => QueueOptions => (dataSource = 'default') => {
  const options = (
    typeof queueOptions === 'function'
      ? queueOptions(dataSource)
      : queueOptions
  ) || {};
  if (options.concurrency) {
    // concurrency specified in cube.js
    return options;
  } else {
    const envConcurrency: number = getEnv('concurrency');
    if (envConcurrency) {
      // concurrency specified in CUBEJS_CONCURRENCY
      return {
        ...options,
        concurrency: envConcurrency,
      };
    } else {
      const defConcurrency = driverService.getDriverConcurrency({
        ...context,
        dataSource,
      });
      if (defConcurrency) {
        // concurrency specified in driver
        return {
          ...options,
          concurrency: defConcurrency,
        };
      }
      // no specified concurrency
      return {
        ...options,
        concurrency: 2,
      };
    }
  }
};

/**
 * Decorate `OrchestratorOptions` with `queueOptions` property which include
 * concurrency calculation logic.
 */
export const decorateOpts = (
  context: RequestContext,
  orchestratorOptions: OrchestratorOptions | (() => OrchestratorOptions),
) => {
  // query queue
  orchestratorOptions.queryCacheOptions =
    orchestratorOptions.queryCacheOptions || {};
  orchestratorOptions.queryCacheOptions.queueOptions = queueOptionsWrapper(
    context,
    orchestratorOptions.queryCacheOptions.queueOptions,
  );
  // pre-aggs queue
  orchestratorOptions.preAggregationsOptions =
    orchestratorOptions.preAggregationsOptions || {};
  orchestratorOptions.preAggregationsOptions.queueOptions = queueOptionsWrapper(
    context,
    orchestratorOptions.preAggregationsOptions.queueOptions,
  );
};

/**
 * Evaluate and returns minimal QueryQueue concurrency value.
 */
export const getSchedulerConcurrency = (
  core: CubejsServerCore,
  context: RequestContext,
): null | number => {
  const queryQueues = core
    .getOrchestratorApi(context)
    .getQueryOrchestrator()
    .getQueryCache()
    .getQueues();

  const preaggsQueues = core
    .getOrchestratorApi(context)
    .getQueryOrchestrator()
    .getPreAggregations()
    .getQueues();

  let concurrency: null | number;

  if (!queryQueues && !preaggsQueues) {
    // first execution - no queues
    concurrency = null;
  } else {
    // further executions - queues ready
    const concurrencies: number[] = [];
    Object.keys(queryQueues).forEach((name) => {
      concurrencies.push(queryQueues[name].concurrency);
    });
    Object.keys(preaggsQueues).forEach((name) => {
      concurrencies.push(preaggsQueues[name].concurrency);
    });
    concurrency = Math.min(...concurrencies);
  }
  return concurrency;
};
