import { BaseDriver } from '@cubejs-backend/query-orchestrator';
import {
  CreateOptions,
  LoggerFn,
  DbTypeFn,
  DriverFactoryFn,
  DriverContext,
  DatabaseType,
  DriverConfig,
  OrchestratorOptions,
} from './types';
import { CubejsServerCore } from './server';

/**
 * Logger instance.
 */
let logger: LoggerFn;

/**
 * Core options.
 */
let coreOptions: CreateOptions & {
  driverFactory: DriverFactoryFn;
  dbType: DbTypeFn;
};

/**
 * Service logger setter.
 */
export const setLogger = (loggerFn: LoggerFn) => {
  logger = loggerFn;
};

/**
 * Asserts incoming options combined with environment.
 */
const assertOptions = (opts: CreateOptions) => {
  if (!opts.driverFactory && !opts.dbType && !process.env.CUBEJS_DB_TYPE) {
    throw new Error('Database type missed');
  }
  if (opts.dbType) {
    logger(
      'Cube.js `dbType` Property Deprecation',
      {
        warning: (
          'dbType property is now deprecated, please migrate: ' +
          'https://github.com/cube-js/cube.js/blob/master/DEPRECATION.md#dbType'
        ),
      },
    );
  }
};

/**
 * Default database factory function.
 */
// eslint-disable-next-line @typescript-eslint/no-unused-vars
const defaultDriverFactory = (ctx: DriverContext): DriverConfig => ({
  type: <DatabaseType>process.env.CUBEJS_DB_TYPE,
  options: {},
});

const getDriver = (val: DriverConfig | BaseDriver) => {
  if (val instanceof BaseDriver) {
    logger(
      'Cube.js `driverFactory` Property Deprecation',
      {
        warning: (
          'driverFactory should return DriverConfig object instead of driver instance, please migrate: ' +
            'https://github.com/cube-js/cube.js/blob/master/DEPRECATION.md#driverFactory'
        ),
      },
    );
    return <BaseDriver>val;
  } else if (
    val.type && typeof val.type === 'string' &&
      val.options && typeof val.options === 'object'
  ) {
    return <DriverConfig>val;
  } else {
    throw new Error(
      'Unexpected driverFactory result value. Must be either DriverConfig or driver instance'
    );
  }
};

/**
 * Driver factory getter.
 */
const getDriverFactory = (opts: CreateOptions): DriverFactoryFn => {
  const { driverFactory } = opts;
  return (ctx: DriverContext) => {
    if (!driverFactory) {
      return defaultDriverFactory(ctx);
    } else if (typeof driverFactory === 'function') {
      const res = driverFactory(ctx);
      if (res instanceof Promise) {
        return res.then((val) => getDriver(val));
      } else {
        return getDriver(res);
      }
    } else {
      throw new Error('Unexpected driverFactory type');
    }
  };
};

/**
 * Driver type getter.
 */
const getDbType = (opts: CreateOptions) => {
  const { dbType, driverFactory } = opts;
  return (ctx: DriverContext) => {
    if (!dbType) {
      const val = driverFactory(ctx);
      if (val instanceof BaseDriver || val instanceof Promise) {
        throw new Error('dbType/driverFactory misconfiguration');
      } else {
        return val.type;
      }
    } else if (typeof dbType === 'function') {
      return dbType(ctx);
    } else if (typeof dbType === 'string') {
      return dbType;
    } else {
      throw new Error('Unexpected dbType type');
    }
  };
};

/**
 * Decorate incomming `CreateOptions` with `dbType` and `driverFactory`
 * properties.
 */
export const decorateOpts = (opts: CreateOptions) => {
  assertOptions(opts);
  opts.driverFactory = getDriverFactory(opts);
  opts.dbType = getDbType(opts) as unknown as DbTypeFn;
  coreOptions = opts as CreateOptions & {
    driverFactory: DriverFactoryFn;
    dbType: DbTypeFn;
  };
};

/**
 * Calculate and returns driver's max pool number.
 */
export const getDriverMaxPool = (
  context: DriverContext,
  options?: OrchestratorOptions,
): undefined | number => {
  if (!options) {
    return undefined;
  } else {
    const queryQueueOptions = (
        options.queryCacheOptions.queueOptions as ((dataSource: String) => {
          concurrency: number,
        })
    )(context.dataSource);
  
    const preAggregationsQueueOptions = (
        options.preAggregationsOptions.queueOptions as ((dataSource: String) => {
          concurrency: number,
        })
    )(context.dataSource);
  
    return 2 * (
      queryQueueOptions.concurrency +
        preAggregationsQueueOptions.concurrency
    );
  }
};

/**
 * Returns default driver concurrency if specified.
 */
export const getDriverConcurrency = (ctx: DriverContext): undefined | number => {
  const type = coreOptions.dbType(ctx);
  const DriverConstructor = CubejsServerCore.lookupDriverClass(type);
  if (
    DriverConstructor &&
    DriverConstructor.getDefaultConcurrency
  ) {
    return DriverConstructor.getDefaultConcurrency();
  }
  return undefined;
};

/**
 * Resolve driver to the data source.
 */
export const resolveDriver = async (
  context: DriverContext,
  options?: OrchestratorOptions,
): Promise<BaseDriver> => {
  const val = await coreOptions.driverFactory(context);
  if (val instanceof BaseDriver) {
    return val;
  } else {
    const type = coreOptions.dbType(context);
    return CubejsServerCore.createDriver(type, {
      maxPoolSize: getDriverMaxPool(context, options),
      ...val.options
    });
  }
};
