import cloneDeep from 'lodash.clonedeep';
import { BaseDriver } from '@cubejs-backend/query-orchestrator';
import { getEnv } from '@cubejs-backend/shared';
import {
  RequestContext,
  DriverContext,

  CreateOptions,
  InitializedOptions,

  DbTypeAsyncFn,
  DriverFactoryAsyncFn,

  DatabaseType,
  DriverConfig,

  OrchestratorOptions,
  OrchestratorInitedOptions,

  QueueOptions,
} from './types';
import { CubejsServerCore } from './server';

/**
 * Driver service class.
 */
export class OptsHelper {
  /**
   * Class constructor.
   */
  public constructor(
    private core: CubejsServerCore,
    private createOptions: CreateOptions,
  ) {
    this._assertOptions(createOptions);
    this._initializedOptions = cloneDeep(this.createOptions);
    this._initializedOptions.driverFactory = this._getDriverFactory(this._initializedOptions);
    this._initializedOptions.dbType = this._getDbType(this._initializedOptions);
  }

  /**
   * Decorated dbType flag.
   */
  private _decoratedType = false;

  /**
   * Decorated driverFactory flag.
   */
  private _decoratedFactory = false;

  /**
   * Initialized options.
   */
  private _initializedOptions: InitializedOptions;

  /**
   * Assert create options.
   */
  private _assertOptions(opts: CreateOptions) {
    if (!process.env.CUBEJS_DB_TYPE && !opts.dbType && !opts.driverFactory) {
      throw new Error(
        'Either CUBEJS_DB_TYPE, CreateOptions.dbType or CreateOptions.driverFactory ' +
        'must be specified'
      );
    }
    if (
      opts.dbType &&
      typeof opts.dbType !== 'string' &&
      typeof opts.dbType !== 'function'
    ) {
      throw new Error(`Unexpected CreateOptions.dbType type: ${
        typeof opts.dbType
      }`);
    }
    if (opts.driverFactory && typeof opts.driverFactory !== 'function') {
      throw new Error(`Unexpected CreateOptions.driverFactory type: ${
        typeof opts.driverFactory
      }`);
    }
    if (opts.dbType) {
      this.core.logger(
        'Cube.js `CreateOptions.dbType` Property Deprecation',
        {
          warning: (
            'CreateOptions.dbType property is now deprecated, please migrate: ' +
            'https://github.com/cube-js/cube.js/blob/master/DEPRECATION.md#dbType'
          ),
        },
      );
    }
  }

  /**
   * Assert value returned from the driver factory.
   */
  private _assertDriverFactoryResult(val: DriverConfig | BaseDriver) {
    if (val instanceof BaseDriver) {
      if (this._decoratedType) {
        throw new Error(
          'CreateOptions.dbType is required if CreateOptions.driverFactory ' +
          'returns driver instance'
        );
      }
      // TODO (buntarb): this can be logged multiple times.
      this.core.logger(
        'Cube.js CreateOptions.driverFactory Property Deprecation',
        {
          warning: (
            'CreateOptions.driverFactory should return DriverConfig object instead of driver instance, please migrate: ' +
            'https://github.com/cube-js/cube.js/blob/master/DEPRECATION.md#driverFactory'
          ),
        },
      );
      return <BaseDriver>val;
    } else if (
      val && val.type && typeof val.type === 'string'
    ) {
      return <DriverConfig>val;
    } else {
      throw new Error(
        'Unexpected CreateOptions.driverFactory result value. Must be either ' +
        `DriverConfig or driver instance: <${typeof val}>${val}`
      );
    }
  }

  /**
   * Assert value returned from the dbType function.
   */
  private _assertDbTypeResult(val: DatabaseType) {
    if (typeof val !== 'string') {
      throw new Error(`Unexpected CreateOptions.dbType result type: <${
        typeof val
      }>${val}`);
    }
    return val;
  }

  /**
   * Default database factory function.
   */ // eslint-disable-next-line @typescript-eslint/no-unused-vars
  private _defaultDriverFactory(ctx: DriverContext): DriverConfig {
    return {
      type: <DatabaseType>process.env.CUBEJS_DB_TYPE,
    };
  }

  /**
   * Async driver factory getter.
   */
  private _getDriverFactory(opts: CreateOptions): DriverFactoryAsyncFn {
    const { dbType, driverFactory } = opts;
    this._decoratedType = !dbType;
    this._decoratedFactory = !driverFactory;

    return async (ctx: DriverContext) => {
      if (!driverFactory) {
        return this._defaultDriverFactory(ctx);
      } else {
        return this._assertDriverFactoryResult(
          await driverFactory(ctx),
        );
      }
    };
  }

  /**
   * Async driver type getter.
   */
  private _getDbType(
    opts: CreateOptions & {
      driverFactory: DriverFactoryAsyncFn,
    },
  ): DbTypeAsyncFn {
    const { dbType, driverFactory } = opts;
    return async (ctx: DriverContext) => {
      if (!dbType) {
        const { type } = <DriverConfig>(await driverFactory(ctx));
        return type;
      } else if (typeof dbType === 'function') {
        return this._assertDbTypeResult(await dbType(ctx));
      } else {
        return dbType;
      }
    };
  }

  /**
   * Returns default driver concurrency if specified.
   */
  private async _getDriverConcurrency(
    ctx: DriverContext
  ): Promise<undefined | number> {
    const type = await this
      .getInitializedOptions()
      .dbType(ctx);
    const DriverConstructor = CubejsServerCore.lookupDriverClass(type);
    if (
      DriverConstructor &&
      DriverConstructor.getDefaultConcurrency
    ) {
      return DriverConstructor.getDefaultConcurrency();
    }
    return undefined;
  }

  /**
   * Wrap queueOptions into a function which evaluate concurrency on the fly.
   */
  private _queueOptionsWrapper(
    context: RequestContext,
    queueOptions: unknown | ((dataSource?: string) => QueueOptions),
  ): (dataSource?: string) => Promise<QueueOptions> {
    return async (dataSource = 'default') => {
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
          const defConcurrency = await this._getDriverConcurrency({
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
  }

  /**
   * Returns server core initialized options object.
   */
  public getInitializedOptions(): InitializedOptions {
    return this._initializedOptions;
  }

  /**
   * Decorate `OrchestratorOptions` with `queueOptions` property which include
   * concurrency calculation logic.
   */
  public getOrchestratorInitializedOptions(
    context: RequestContext,
    orchestratorOptions: OrchestratorOptions,
  ): OrchestratorInitedOptions {
    const clone = cloneDeep(orchestratorOptions);
    // query queue
    clone.queryCacheOptions = clone.queryCacheOptions || {};
    clone.queryCacheOptions.queueOptions = this._queueOptionsWrapper(
      context,
      clone.queryCacheOptions.queueOptions,
    );
    // pre-aggs queue
    clone.preAggregationsOptions = clone.preAggregationsOptions || {};
    clone.preAggregationsOptions.queueOptions = this._queueOptionsWrapper(
      context,
      clone.preAggregationsOptions.queueOptions,
    );
    return clone;
  }
}
