/**
 * @license Apache-2.0
 * @copyright Cube Dev, Inc.
 * @fileoverview Cube.js concurrencies calculation logic declaration.
 */

import type { DatabaseType, ExternalDbTypeFn } from '../types';
import type { CompilerApi } from '../CompilerApi';
import { lookupDriverClass } from './DriverLookup';

/**
 * upgradeConcurrency() promise holder.
 * @private
 */
let concurrencyPromise: Promise<void>;

/**
 * System pre-aggs concurrency value.
 * @private
 */
let _preaggs: number;

/**
 * Returns mono concurrency value.
 * @private
 */
function getMonoConcurrency(dbType?: DatabaseType | ExternalDbTypeFn): number {
  if (process.env.CUBEJS_MONO_CONCURRENCY) {
    return parseInt(process.env.CUBEJS_MONO_CONCURRENCY, 10);
  } else if (dbType) {
    const Driver = lookupDriverClass(dbType);
    return Driver.monoConcurrencyDefault
      ? Driver.monoConcurrencyDefault()
      : 1;
  } else {
    return 1;
  }
}

/**
 * Returns concurrency object.
 * @private
 */
function calcConcurrency(opt: {
  dbType?: DatabaseType | ExternalDbTypeFn,
  monoConcurrency: number,
  forcePreaggs?: boolean,
}): {
  maxpool: number;
  queries: number;
  preaggs: number;
} {
  if (
    opt.dbType &&
    lookupDriverClass(opt.dbType).calcConcurrency
  ) {
    return lookupDriverClass(opt.dbType).calcConcurrency(
      opt.monoConcurrency,
      opt.forcePreaggs,
    );
  } else {
    switch (opt.monoConcurrency) {
      default:
        return {
          maxpool: 4,
          queries: 2,
          preaggs: undefined,
        };
    }
  }
}

/**
 * Returns concurrency object for specified dbType.
 */
export function getConcurrency(dbType?: DatabaseType | ExternalDbTypeFn): {
  maxpool: number;
  queries: number;
  preaggs: number;
} {
  return {
    ...calcConcurrency({
      dbType,
      monoConcurrency: getMonoConcurrency(dbType),
      forcePreaggs: false,
    }),
    preaggs: _preaggs,
  };
}

/**
 * Updates the value of the workers number based on the stored cubes
 * configurations.
 */
export async function upgradeConcurrency(compilerApi: CompilerApi) {
  if (!concurrencyPromise) {
    concurrencyPromise = new Promise((resolve) => {
      let result: number;
      compilerApi
        .getCompilers({ requestId: 'upgrade.concurrency' })
        .then((compilers) => {
          const { cubeEvaluator } = compilers;
          cubeEvaluator.cubeNames().forEach((name) => {
            const dbType = compilerApi.getDbType(
              cubeEvaluator.cubeFromPath(name).dataSource ?? 'default',
            );
            const monoConcurrency = getMonoConcurrency(dbType);
            const { preaggs } = calcConcurrency({
              dbType,
              monoConcurrency,
              forcePreaggs: false,
            });
            // eslint-disable-next-line no-nested-ternary
            result = typeof result === 'number'
              ? result > preaggs ? preaggs : result
              : preaggs;
          });
          _preaggs = result;
          concurrencyPromise = undefined;
          resolve();
        });
    });
  }
  return concurrencyPromise;
}
