import fs from 'fs-extra';
import path from 'path';
import { Constructor } from '@cubejs-backend/shared';
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
  } else if (
    fs.existsSync(path.join('node_modules', `${dbType}-cubejs-driver`))
  ) {
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
