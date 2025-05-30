import { Constructor, packageExists } from '@cubejs-backend/shared';
import { BaseDriver } from '@cubejs-backend/query-orchestrator';
import {
  DatabaseType,
  DriverOptions,
  DriverContext,
  OrchestratorInitedOptions,
} from './types';
import DriverDependencies from './DriverDependencies';

/**
 * Resolve driver module name by db type.
 */
export const driverDependencies = (dbType: DatabaseType) => {
  if (DriverDependencies[dbType]) {
    return DriverDependencies[dbType];
  }

  if (packageExists(`@cubejs-backend/${dbType}-driver`, true)) {
    return `@cubejs-backend/${dbType}-driver`;
  }

  if (packageExists(`${dbType}-cubejs-driver`, true)) {
    return `${dbType}-cubejs-driver`;
  }

  throw new Error(`Unsupported db type: ${dbType}`);
};

/**
 * Resolve driver module object by db type.
 */
export const lookupDriverClass = (dbType): Constructor<BaseDriver> & {
  dialectClass?: () => any;
  getDefaultConcurrency?: () => number;
} => {
  // eslint-disable-next-line global-require,import/no-dynamic-require
  const module = require(
    driverDependencies(dbType || process.env.CUBEJS_DB_TYPE)
  );
  if (module.default) {
    return module.default;
  }
  return module;
};

/**
 * Determines whether specified value is a BaseDriver instance or not.
 */
export const isDriver = (val: any): boolean => {
  let isDriverInstance = val instanceof BaseDriver;
  if (!isDriverInstance && val?.constructor) {
    let end = false;
    let obj = val.constructor;
    while (!isDriverInstance && !end) {
      obj = Object.getPrototypeOf(obj);
      end = !obj;
      isDriverInstance = obj?.name ? obj.name === 'BaseDriver' : false;
    }
  }
  return isDriverInstance;
};

/**
 * Create new driver instance by specified database type.
 */
export const createDriver = (
  type: DatabaseType,
  options?: DriverOptions,
): BaseDriver => new (lookupDriverClass(type))(options);

/**
 * Calculate and returns driver's max pool number.
 */
export const getDriverMaxPool = async (
  context: DriverContext,
  options?: OrchestratorInitedOptions,
): Promise<undefined | number> => {
  if (!options) {
    return undefined;
  } else {
    const queryQueueOptions = await options
      .queryCacheOptions
      .queueOptions(context.dataSource);

    const preAggregationsQueueOptions = await options
      .preAggregationsOptions
      .queueOptions(context.dataSource);

    return 2 * (
      queryQueueOptions.concurrency +
        preAggregationsQueueOptions.concurrency
    );
  }
};
