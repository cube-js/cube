import crypto from 'crypto';
import fs from 'fs-extra';
import path from 'path';
import cloneDeep from 'lodash.clonedeep';
import { BaseDriver } from '@cubejs-backend/query-orchestrator';
import {
  getEnv,
  isDockerImage,
  displayCLIWarning,
} from '@cubejs-backend/shared';
import {
  CreateOptions,
  SystemOptions,
  DriverDecoratedOptions,
  ServerCoreInitializedOptions,
  RequestContext,
  DriverContext,
  DbTypeAsyncFn,
  DriverFactoryAsyncFn,
  DatabaseType,
  DriverConfig,
  OrchestratorOptions,
  OrchestratorInitedOptions,
  QueueOptions,
} from './types';
import { lookupDriverClass } from './DriverResolvers';
import type { CubejsServerCore } from './server';
import optionsValidate from './optionsValidate';

const { version } = require('../../../package.json');

/**
 * Driver service class.
 */
export class OptsHandler {
  /**
   * Class constructor.
   */
  public constructor(
    private core: CubejsServerCore,
    private createOptions: CreateOptions,
    private systemOptions?: SystemOptions,
  ) {
    this.assertOptions(createOptions);
    const options = cloneDeep(this.createOptions);
    options.driverFactory = this.getDriverFactory(options);
    options.dbType = this.getDbType(options);
    this.initializedOptions = this.initializeCoreOptions(options);
  }

  /**
   * Decorated dbType flag.
   */
  private decoratedType = false;

  /**
   * Decorated driverFactory flag.
   */
  private decoratedFactory = false;

  /**
   * Initialized options.
   */
  private initializedOptions: ServerCoreInitializedOptions;

  /**
   * Assert create options.
   */
  private assertOptions(opts: CreateOptions) {
    optionsValidate(opts);

    if (
      !this.configuredAsDevServer() &&
      !process.env.CUBEJS_DB_TYPE &&
      !opts.dbType &&
      !opts.driverFactory
    ) {
      throw new Error(
        'Either CUBEJS_DB_TYPE, CreateOptions.dbType or CreateOptions.driverFactory ' +
        'must be specified'
      );
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
  private assertDriverFactoryResult(val: DriverConfig | BaseDriver) {
    if (val instanceof BaseDriver) {
      if (this.decoratedType) {
        throw new Error(
          'CreateOptions.dbType is required if CreateOptions.driverFactory ' +
          'returns driver instance'
        );
      }
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
  private assertDbTypeResult(val: DatabaseType) {
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
  private defaultDriverFactory(ctx: DriverContext): DriverConfig {
    return {
      type: <DatabaseType>process.env.CUBEJS_DB_TYPE,
    };
  }

  /**
   * Async driver factory getter.
   */
  private getDriverFactory(opts: CreateOptions): DriverFactoryAsyncFn {
    const { dbType, driverFactory } = opts;
    this.decoratedType = !dbType;
    this.decoratedFactory = !driverFactory;

    return async (ctx: DriverContext) => {
      if (!driverFactory) {
        return this.defaultDriverFactory(ctx);
      } else {
        return this.assertDriverFactoryResult(
          await driverFactory(ctx),
        );
      }
    };
  }

  /**
   * Async driver type getter.
   */
  private getDbType(
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
        return this.assertDbTypeResult(await dbType(ctx));
      } else {
        return dbType;
      }
    };
  }

  /**
   * Returns default driver concurrency if specified.
   */
  private async getDriverConcurrency(
    ctx: DriverContext
  ): Promise<undefined | number> {
    const type = await this
      .getCoreInitializedOptions()
      .dbType(ctx);
    const DriverConstructor = lookupDriverClass(type);
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
  private queueOptionsWrapper(
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
          const defConcurrency = await this.getDriverConcurrency({
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
   * Initialize core options.
   */
  private initializeCoreOptions(
    opts: DriverDecoratedOptions
  ): ServerCoreInitializedOptions {
    const skipOnEnv = [
      // Default EXT_DB variables
      'CUBEJS_EXT_DB_URL',
      'CUBEJS_EXT_DB_HOST',
      'CUBEJS_EXT_DB_NAME',
      'CUBEJS_EXT_DB_PORT',
      'CUBEJS_EXT_DB_USER',
      'CUBEJS_EXT_DB_PASS',
      // Cube Store variables
      'CUBEJS_CUBESTORE_HOST',
      'CUBEJS_CUBESTORE_PORT',
      'CUBEJS_CUBESTORE_USER',
      'CUBEJS_CUBESTORE_PASS',
    ];

    const definedExtDBVariables =
      skipOnEnv.filter((field) => process.env[field] !== undefined);

    const externalDbType =
      opts.externalDbType ||
      <DatabaseType | undefined>process.env.CUBEJS_EXT_DB_TYPE ||
      (getEnv('devMode') || definedExtDBVariables.length > 0) && 'cubestore' ||
      undefined;

    const devServer =
      process.env.NODE_ENV !== 'production' ||
      getEnv('devMode');

    let externalDriverFactory =
      externalDbType &&
      (
        () => new (lookupDriverClass(externalDbType))({
          url: process.env.CUBEJS_EXT_DB_URL,
          host: process.env.CUBEJS_EXT_DB_HOST,
          database: process.env.CUBEJS_EXT_DB_NAME,
          port: process.env.CUBEJS_EXT_DB_PORT,
          user: process.env.CUBEJS_EXT_DB_USER,
          password: process.env.CUBEJS_EXT_DB_PASS,
        })
      );

    let externalDialectFactory =
      () => typeof externalDbType === 'string' &&
      lookupDriverClass(externalDbType).dialectClass &&
      lookupDriverClass(externalDbType).dialectClass();

    if (!devServer && getEnv('externalDefault') && !externalDbType) {
      displayCLIWarning(
        'Cube Store is not found. Please follow this documentation ' +
        'to configure Cube Store ' +
        'https://cube.dev/docs/caching/running-in-production'
      );
    }

    if (devServer && externalDbType !== 'cubestore') {
      displayCLIWarning(
        `Using ${externalDbType} as an external database is deprecated. ` +
        'Please use Cube Store instead: ' +
        'https://cube.dev/docs/caching/running-in-production'
      );
    }

    if (externalDbType === 'cubestore' && devServer && !opts.serverless) {
      if (!definedExtDBVariables.length) {
        // TODO (buntarb): why can't we import this in a regular way?
        
        // Old note: requireFromPackage was used here. Removed as it wasn't
        // necessary check and conflicts with local E2E test running.

        // eslint-disable-next-line global-require,import/no-extraneous-dependencies
        const cubeStorePackage = require('@cubejs-backend/cubestore-driver');
        if (cubeStorePackage.isCubeStoreSupported()) {
          const cubeStoreHandler = new cubeStorePackage.CubeStoreHandler({
            stdout: (data) => {
              console.log(data.toString().trim());
            },
            stderr: (data) => {
              console.log(data.toString().trim());
            },
            onRestart: (code) => this.core.logger('Cube Store Restarting', {
              warning: `Instance exit with ${code}, restarting`,
            }),
          });

          console.log(`🔥 Cube Store (${version}) is assigned to 3030 port.`);

          // Start Cube Store on startup in official docker images
          if (isDockerImage()) {
            cubeStoreHandler.acquire().catch(
              (e) => this.core.logger('Cube Store Start Error', {
                error: e.message,
              })
            );
          }

          // Lazy loading for Cube Store
          externalDriverFactory =
            () => new cubeStorePackage.CubeStoreDevDriver(cubeStoreHandler);
          externalDialectFactory =
            () => cubeStorePackage.CubeStoreDevDriver.dialectClass();
        } else {
          this.core.logger('Cube Store is not supported on your system', {
            warning: (
              `You are using ${
                process.platform
              } platform with ${
                process.arch
              } architecture, which is not supported by Cube Store.`
            ),
          });
        }
      }
    }

    const options: ServerCoreInitializedOptions = {
      externalDbType,
      devServer,
      dialectFactory: (ctx) => (
        lookupDriverClass(ctx.dbType).dialectClass &&
        lookupDriverClass(ctx.dbType).dialectClass()
      ),
      externalDriverFactory,
      externalDialectFactory,
      apiSecret: process.env.CUBEJS_API_SECRET,
      telemetry: getEnv('telemetry'),
      scheduledRefreshTimeZones:
        process.env.CUBEJS_SCHEDULED_REFRESH_TIMEZONES &&
        process.env.CUBEJS_SCHEDULED_REFRESH_TIMEZONES.split(',').map(t => t.trim()),
      scheduledRefreshContexts: async () => [null],
      basePath: '/cubejs-api',
      dashboardAppPath: 'dashboard-app',
      dashboardAppPort: 3000,
      scheduledRefreshConcurrency:
        parseInt(process.env.CUBEJS_SCHEDULED_REFRESH_CONCURRENCY, 10),
      preAggregationsSchema:
        getEnv('preAggregationsSchema') ||
        (devServer ? 'dev_pre_aggregations' : 'prod_pre_aggregations'),
      schemaPath: process.env.CUBEJS_SCHEMA_PATH || 'schema',
      scheduledRefreshTimer: getEnv('refreshWorkerMode'),
      sqlCache: true,
      livePreview: getEnv('livePreview'),
      ...opts,
      jwt: {
        key: getEnv('jwtKey'),
        algorithms: getEnv('jwtAlgorithms'),
        issuer: getEnv('jwtIssuer'),
        audience: getEnv('jwtAudience'),
        subject: getEnv('jwtSubject'),
        jwkUrl: getEnv('jwkUrl'),
        claimsNamespace: getEnv('jwtClaimsNamespace'),
        ...opts.jwt,
      }
    };

    if (opts.contextToAppId && !opts.scheduledRefreshContexts) {
      this.core.logger('Multitenancy Without ScheduledRefreshContexts', {
        warning: (
          'You are using multitenancy without configuring scheduledRefreshContexts, ' +
          'which can lead to issues where the security context will be undefined ' +
          'while Cube.js will do background refreshing: ' +
          'https://cube.dev/docs/config#options-reference-scheduled-refresh-contexts'
        ),
      });
    }

    if (options.devServer && !options.apiSecret) {
      options.apiSecret = crypto.randomBytes(16).toString('hex');
      displayCLIWarning(
        `Option apiSecret is required in dev mode. Cube.js has generated it as ${
          options.apiSecret
        }`
      );
    }

    // Create schema directory to protect error on new project with dev mode
    // (docker flow)
    if (options.devServer) {
      const repositoryPath = path.join(process.cwd(), options.schemaPath);
      if (!fs.existsSync(repositoryPath)) {
        fs.mkdirSync(repositoryPath);
      }
    }

    if (!options.devServer || this.configuredForQueryProcessing()) {
      const fieldsForValidation: (keyof ServerCoreInitializedOptions)[] = [
        'driverFactory',
        'dbType'
      ];

      if (!options.jwt?.jwkUrl) {
        // apiSecret is required only for auth by JWT, for JWK it's not needed
        fieldsForValidation.push('apiSecret');
      }

      const invalidFields =
        fieldsForValidation.filter((field) => options[field] === undefined);
      if (invalidFields.length) {
        throw new Error(
          `${
            invalidFields.join(', ')
          } ${
            invalidFields.length === 1 ? 'is' : 'are'
          } required option(s)`
        );
      }
    }

    return options;
  }

  /**
   * Determines whether current instance should be bootstraped in the
   * dev mode or not.
   */
  public configuredAsDevServer(): boolean {
    return (
      this.createOptions.devServer ||
      process.env.NODE_ENV !== 'production' ||
      getEnv('devMode')
    );
  }

  /**
   * Determines whether current configuration is sutisfied system to process
   * queries.
   */
  public configuredForQueryProcessing(): boolean {
    const hasDbCredentials =
      Object.keys(process.env).filter(
        (key) => (
          key.startsWith('CUBEJS_DB') && key !== 'CUBEJS_DB_TYPE' ||
          key.startsWith('CUBEJS_AWS')
        )
      ).length > 0;

    return (
      hasDbCredentials ||
      this.systemOptions?.isCubeConfigEmpty === undefined ||
      !this.systemOptions?.isCubeConfigEmpty
    );
  }

  /**
   * Returns server core initialized options object.
   */
  public getCoreInitializedOptions(): ServerCoreInitializedOptions {
    return this.initializedOptions;
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
    clone.queryCacheOptions.queueOptions = this.queueOptionsWrapper(
      context,
      clone.queryCacheOptions.queueOptions,
    );
    // pre-aggs queue
    clone.preAggregationsOptions = clone.preAggregationsOptions || {};
    clone.preAggregationsOptions.queueOptions = this.queueOptionsWrapper(
      context,
      clone.preAggregationsOptions.queueOptions,
    );
    return clone;
  }
}
