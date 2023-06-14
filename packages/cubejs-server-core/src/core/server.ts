/* eslint-disable global-require,no-return-assign */
import crypto from 'crypto';
import fs from 'fs-extra';
import LRUCache from 'lru-cache';
import isDocker from 'is-docker';
import pLimit from 'p-limit';

import { ApiGateway, ApiGatewayOptions, UserBackgroundContext } from '@cubejs-backend/api-gateway';
import {
  CancelableInterval,
  createCancelableInterval, formatDuration, getAnonymousId,
  getEnv, assertDataSource, getRealType, internalExceptions, track, FileRepository, SchemaFileRepository,
} from '@cubejs-backend/shared';

import type { Application as ExpressApplication } from 'express';

import { BaseDriver, DriverFactoryByDataSource } from '@cubejs-backend/query-orchestrator';
import { RefreshScheduler, ScheduledRefreshOptions } from './RefreshScheduler';
import { OrchestratorApi, OrchestratorApiOptions } from './OrchestratorApi';
import { CompilerApi } from './CompilerApi';
import { DevServer } from './DevServer';
import agentCollect from './agentCollect';
import { OrchestratorStorage } from './OrchestratorStorage';
import { prodLogger, devLogger } from './logger';
import { OptsHandler } from './OptsHandler';
import {
  driverDependencies,
  lookupDriverClass,
  isDriver,
  createDriver,
  getDriverMaxPool,
} from './DriverResolvers';

import type {
  CreateOptions,
  SystemOptions,
  ServerCoreInitializedOptions,
  ContextToAppIdFn,
  DatabaseType,
  DbTypeAsyncFn,
  ExternalDbTypeFn,
  OrchestratorOptionsFn,
  OrchestratorInitedOptions,
  PreAggregationsSchemaFn,
  RequestContext,
  DriverContext,
  LoggerFn,
  DriverConfig,
} from './types';
import { ContextToOrchestratorIdFn, ContextAcceptanceResult, ContextAcceptanceResultHttp, ContextAcceptanceResultWs, ContextAcceptor } from './types';

const { version } = require('../../../package.json');

function wrapToFnIfNeeded<T, R>(possibleFn: T | ((a: R) => T)): (a: R) => T {
  if (typeof possibleFn === 'function') {
    return <any>possibleFn;
  }

  return () => possibleFn;
}

class AcceptAllAcceptor {
  public shouldAccept(): ContextAcceptanceResult {
    return { accepted: true };
  }

  public shouldAcceptHttp(): ContextAcceptanceResultHttp {
    return { accepted: true };
  }

  public shouldAcceptWs(): ContextAcceptanceResultWs {
    return { accepted: true };
  }
}

export class CubejsServerCore {
  /**
   * Returns core version based on package.json.
   */
  public static version() {
    return version;
  }

  /**
   * Resolve driver module name by db type.
   */
  public static driverDependencies = driverDependencies;

  /**
   * Resolve driver module object by db type.
   */
  public static lookupDriverClass = lookupDriverClass;

  /**
   * Create new driver instance by specified database type.
   */
  public static createDriver = createDriver;

  /**
   * Calculate and returns driver's max pool number.
   */
  public static getDriverMaxPool = getDriverMaxPool;

  public repository: FileRepository;

  protected devServer: DevServer | undefined;

  protected readonly orchestratorStorage: OrchestratorStorage = new OrchestratorStorage();

  protected repositoryFactory: ((context: RequestContext) => SchemaFileRepository) | (() => FileRepository);

  protected contextToDbType: DbTypeAsyncFn;

  protected contextToExternalDbType: ExternalDbTypeFn;

  protected compilerCache: LRUCache<string, CompilerApi>;

  protected readonly contextToOrchestratorId: ContextToOrchestratorIdFn;

  protected readonly preAggregationsSchema: PreAggregationsSchemaFn;

  protected readonly orchestratorOptions: OrchestratorOptionsFn;

  public logger: LoggerFn;

  protected optsHandler: OptsHandler;

  protected preAgentLogger: any;

  protected readonly options: ServerCoreInitializedOptions;

  protected readonly contextToAppId: ContextToAppIdFn = () => process.env.CUBEJS_APP || 'STANDALONE';

  protected readonly standalone: boolean = true;

  protected maxCompilerCacheKeep: NodeJS.Timeout | null = null;

  protected scheduledRefreshTimerInterval: CancelableInterval | null = null;

  protected driver: BaseDriver | null = null;

  protected apiGatewayInstance: ApiGateway | null = null;

  public readonly event: (name: string, props?: object) => Promise<void>;

  public projectFingerprint: string | null = null;

  public anonymousId: string | null = null;

  public coreServerVersion: string | null = null;

  private contextAcceptor: ContextAcceptor;

  /**
   * Class constructor.
   */
  public constructor(
    opts: CreateOptions = {},
    protected readonly systemOptions?: SystemOptions,
  ) {
    this.coreServerVersion = version;

    this.logger = opts.logger || (
      process.env.NODE_ENV !== 'production'
        ? devLogger(process.env.CUBEJS_LOG_LEVEL)
        : prodLogger(process.env.CUBEJS_LOG_LEVEL)
    );

    this.optsHandler = new OptsHandler(this, opts, systemOptions);
    this.options = this.optsHandler.getCoreInitializedOptions();

    this.repository = new FileRepository(this.options.schemaPath);
    this.repositoryFactory = this.options.repositoryFactory || (() => this.repository);

    this.contextToDbType = this.options.dbType;
    this.contextToExternalDbType = wrapToFnIfNeeded(this.options.externalDbType);
    this.preAggregationsSchema = wrapToFnIfNeeded(this.options.preAggregationsSchema);
    this.orchestratorOptions = wrapToFnIfNeeded(this.options.orchestratorOptions);

    this.compilerCache = new LRUCache<string, CompilerApi>({
      max: this.options.compilerCacheSize || 250,
      maxAge: this.options.maxCompilerCacheKeepAlive,
      updateAgeOnGet: this.options.updateCompilerCacheKeepAlive
    });

    if (this.options.contextToAppId) {
      this.contextToAppId = this.options.contextToAppId;
      this.standalone = false;
    }

    this.contextAcceptor = this.createContextAcceptor();

    if (this.options.contextToDataSourceId) {
      throw new Error('contextToDataSourceId has been deprecated and removed. Use contextToOrchestratorId instead.');
    }

    this.contextToOrchestratorId = this.options.contextToOrchestratorId || (() => 'STANDALONE');

    // proactively free up old cache values occasionally
    if (this.options.maxCompilerCacheKeepAlive) {
      this.maxCompilerCacheKeep = setInterval(
        () => this.compilerCache.prune(),
        this.options.maxCompilerCacheKeepAlive
      );
    }

    this.startScheduledRefreshTimer();

    this.event = async (name, props) => {
      if (!this.options.telemetry) {
        return;
      }

      if (!this.projectFingerprint) {
        try {
          this.projectFingerprint = crypto.createHash('md5')
            .update(JSON.stringify(fs.readJsonSync('package.json')))
            .digest('hex');
        } catch (e) {
          internalExceptions(e as Error);
        }
      }

      if (!this.anonymousId) {
        this.anonymousId = getAnonymousId();
      }

      const internalExceptionsEnv = getEnv('internalExceptions');

      try {
        await track({
          event: name,
          projectFingerprint: this.projectFingerprint,
          coreServerVersion: this.coreServerVersion,
          dockerVersion: getEnv('dockerImageVersion'),
          isDocker: isDocker(),
          internalExceptions: internalExceptionsEnv !== 'false' ? internalExceptionsEnv : undefined,
          ...props
        });
      } catch (e) {
        internalExceptions(e as Error);
      }
    };

    this.initAgent();

    if (this.options.devServer && !this.isReadyForQueryProcessing()) {
      this.event('first_server_start');
    }

    if (this.options.devServer) {
      this.devServer = new DevServer(this, {
        dockerVersion: getEnv('dockerImageVersion'),
        externalDbTypeFn: this.contextToExternalDbType,
        isReadyForQueryProcessing: this.isReadyForQueryProcessing.bind(this)
      });
      const oldLogger = this.logger;
      this.logger = ((msg, params) => {
        if (
          msg === 'Load Request' ||
          msg === 'Load Request Success' ||
          msg === 'Orchestrator error' ||
          msg === 'Internal Server Error' ||
          msg === 'User Error' ||
          msg === 'Compiling schema' ||
          msg === 'Recompiling schema' ||
          msg === 'Slow Query Warning' ||
          msg === 'Cube SQL Error'
        ) {
          const props = {
            error: params.error,
            ...(params.apiType ? { apiType: params.apiType } : {}),
            ...(params.protocol ? { protocol: params.protocol } : {}),
            ...(params.appName ? { appName: params.appName } : {}),
            ...(params.sanitizedQuery ? { query: params.sanitizedQuery } : {}),
          };

          this.event(msg, props);
        }
        oldLogger(msg, params);
      });

      if (!process.env.CI) {
        process.on('uncaughtException', this.onUncaughtException);
      }
    } else {
      const oldLogger = this.logger;
      let loadRequestCount = 0;
      let loadSqlRequestCount = 0;

      this.logger = ((msg, params) => {
        if (msg === 'Load Request Success') {
          if (params.apiType === 'sql') {
            loadSqlRequestCount++;
          } else {
            loadRequestCount++;
          }
        } else if (msg === 'Cube SQL Error') {
          const props = {
            error: params.error,
            apiType: params.apiType,
            protocol: params.protocol,
            ...(params.appName ? { appName: params.appName } : {}),
            ...(params.sanitizedQuery ? { query: params.sanitizedQuery } : {}),
          };
          this.event(msg, props);
        }
        oldLogger(msg, params);
      });

      setInterval(() => {
        if (loadRequestCount > 0 || loadSqlRequestCount > 0) {
          this.event('Load Request Success Aggregated', { loadRequestSuccessCount: loadRequestCount, loadSqlRequestSuccessCount: loadSqlRequestCount });
        }
        loadRequestCount = 0;
        loadSqlRequestCount = 0;
      }, 60000);

      this.event('Server Start');
    }
  }

  protected createContextAcceptor(): ContextAcceptor {
    return new AcceptAllAcceptor();
  }

  /**
   * Determines whether current instance is ready to process queries.
   */
  protected isReadyForQueryProcessing(): boolean {
    return this.optsHandler.configuredForQueryProcessing();
  }

  public startScheduledRefreshTimer(): [boolean, string | null] {
    if (!this.isReadyForQueryProcessing()) {
      return [false, 'Instance is not ready for query processing, refresh scheduler is disabled'];
    }

    if (this.scheduledRefreshTimerInterval) {
      return [true, null];
    }
    if (this.optsHandler.configuredForScheduledRefresh()) {
      const scheduledRefreshTimer = this.optsHandler.getScheduledRefreshInterval();
      this.scheduledRefreshTimerInterval = createCancelableInterval(
        () => this.handleScheduledRefreshInterval({}),
        {
          interval: scheduledRefreshTimer,
          onDuplicatedExecution: (intervalId) => this.logger('Refresh Scheduler Interval', {
            warning: `Previous interval #${intervalId} was not finished with ${scheduledRefreshTimer} interval`
          }),
          onDuplicatedStateResolved: (intervalId, elapsed) => this.logger('Refresh Scheduler Long Execution', {
            warning: `Interval #${intervalId} finished after ${formatDuration(elapsed)}. Please consider reducing total number of partitions by using rollup_lambda pre-aggregations.`
          })
        }
      );

      return [true, null];
    }

    return [false, 'Instance configured without scheduler refresh timer, refresh scheduler is disabled'];
  }

  /**
   * Reload global variables and updates drivers according to new values.
   *
   * Note: currently there is no way to change CubejsServerCore.options,
   * as so, we are not refreshing CubejsServerCore.options.dbType and
   * CubejsServerCore.options.driverFactory here. If this will be changed,
   * we will need to do this in order to update driver.
   */
  protected reloadEnvVariables() {
    this.driver = null;
    this.options.externalDbType = this.options.externalDbType ||
      <DatabaseType | undefined>process.env.CUBEJS_EXT_DB_TYPE;
    this.options.schemaPath = process.env.CUBEJS_SCHEMA_PATH || this.options.schemaPath;
    this.contextToExternalDbType = wrapToFnIfNeeded(this.options.externalDbType);
  }

  protected initAgent() {
    const agentEndpointUrl = getEnv('agentEndpointUrl');
    if (agentEndpointUrl) {
      const oldLogger = this.logger;
      this.preAgentLogger = oldLogger;
      this.logger = (msg, params) => {
        oldLogger(msg, params);
        agentCollect(
          {
            msg,
            ...params
          },
          agentEndpointUrl,
          oldLogger
        );
      };
    }
  }

  protected async flushAgent() {
    const agentEndpointUrl = getEnv('agentEndpointUrl');
    if (agentEndpointUrl) {
      await agentCollect(
        { msg: 'Flush Agent' },
        agentEndpointUrl,
        this.preAgentLogger
      );
    }
  }

  public async initApp(app: ExpressApplication) {
    const apiGateway = this.apiGateway();
    apiGateway.initApp(app);

    if (this.options.devServer) {
      this.devServer.initDevEnv(app, this.options);
    } else {
      app.get('/', (req, res) => {
        res.status(200)
          .send('<html><body>Cube.js server is running in production mode. <a href="https://cube.dev/docs/deployment/production-checklist">Learn more about production mode</a>.</body></html>');
      });
    }
  }

  public initSubscriptionServer(sendMessage) {
    const apiGateway = this.apiGateway();
    return apiGateway.initSubscriptionServer(sendMessage);
  }

  public initSQLServer() {
    const apiGateway = this.apiGateway();
    return apiGateway.initSQLServer();
  }

  protected apiGateway(): ApiGateway {
    if (this.apiGatewayInstance) {
      return this.apiGatewayInstance;
    }

    return (this.apiGatewayInstance = this.createApiGatewayInstance(
      this.options.apiSecret,
      this.getCompilerApi.bind(this),
      this.getOrchestratorApi.bind(this),
      this.logger,
      {
        standalone: this.standalone,
        dataSourceStorage: this.orchestratorStorage,
        basePath: this.options.basePath,
        checkAuthMiddleware: this.options.checkAuthMiddleware,
        contextRejectionMiddleware: this.contextRejectionMiddleware.bind(this),
        wsContextAcceptor: this.contextAcceptor.shouldAcceptWs.bind(this.contextAcceptor),
        checkAuth: this.options.checkAuth,
        queryRewrite:
          this.options.queryRewrite || this.options.queryTransformer,
        extendContext: this.options.extendContext,
        playgroundAuthSecret: getEnv('playgroundAuthSecret'),
        jwt: this.options.jwt,
        refreshScheduler: this.getRefreshScheduler.bind(this),
        scheduledRefreshContexts: this.options.scheduledRefreshContexts,
        scheduledRefreshTimeZones: this.options.scheduledRefreshTimeZones,
        serverCoreVersion: this.coreServerVersion,
        contextToApiScopes: this.options.contextToApiScopes,
        event: this.event,
      }
    ));
  }

  protected createApiGatewayInstance(
    apiSecret: string,
    getCompilerApi: (context: RequestContext) => Promise<CompilerApi>,
    getOrchestratorApi: (context: RequestContext) => Promise<OrchestratorApi>,
    logger: LoggerFn,
    options: ApiGatewayOptions
  ): ApiGateway {
    return new ApiGateway(apiSecret, getCompilerApi, getOrchestratorApi, logger, options);
  }

  protected async contextRejectionMiddleware(req, res, next) {
    if (!this.standalone) {
      const result = this.contextAcceptor.shouldAcceptHttp(req.context);
      if (!result.accepted) {
        res.writeHead(result.rejectStatusCode!, result.rejectHeaders!);
        res.send();
        return;
      }
    }
    if (next) {
      next();
    }
  }

  public async getCompilerApi(context: RequestContext) {
    const appId = await this.contextToAppId(context);
    let compilerApi = this.compilerCache.get(appId);
    const currentSchemaVersion = this.options.schemaVersion && (() => this.options.schemaVersion(context));

    if (!compilerApi) {
      compilerApi = this.createCompilerApi(
        this.repositoryFactory(context),
        {
          dbType: async (dataSourceContext) => {
            const dbType = await this.contextToDbType({ ...context, ...dataSourceContext });
            return dbType;
          },
          externalDbType: this.contextToExternalDbType(context),
          dialectClass: (dialectContext) => (
            this.options.dialectFactory &&
            this.options.dialectFactory({ ...context, ...dialectContext })
          ),
          externalDialectClass: this.options.externalDialectFactory && this.options.externalDialectFactory(context),
          schemaVersion: currentSchemaVersion,
          preAggregationsSchema: this.preAggregationsSchema(context),
          context,
          allowJsDuplicatePropsInSchema: this.options.allowJsDuplicatePropsInSchema,
          allowNodeRequire: this.options.allowNodeRequire,
        },
      );

      this.compilerCache.set(appId, compilerApi);
    }

    compilerApi.schemaVersion = currentSchemaVersion;
    return compilerApi;
  }

  public async resetInstanceState() {
    await this.orchestratorStorage.releaseConnections();

    this.orchestratorStorage.clear();
    this.compilerCache.reset();

    this.reloadEnvVariables();

    this.repository = new FileRepository(this.options.schemaPath);
    this.repositoryFactory = this.options.repositoryFactory || (() => this.repository);
    
    this.startScheduledRefreshTimer();
  }

  public async getOrchestratorApi(context: RequestContext): Promise<OrchestratorApi> {
    const orchestratorId = await this.contextToOrchestratorId(context);

    if (this.orchestratorStorage.has(orchestratorId)) {
      return this.orchestratorStorage.get(orchestratorId);
    }

    /**
     * Hash table to store promises which will be resolved with the
     * datasource drivers. DriverFactoryByDataSource function is closure
     * this constant.
     */
    const driverPromise: Record<string, Promise<BaseDriver>> = {};

    let externalPreAggregationsDriverPromise: Promise<BaseDriver> | null = null;

    const contextToDbType: DbTypeAsyncFn = this.contextToDbType.bind(this);
    const externalDbType = this.contextToExternalDbType(context);

    // orchestrator options can be empty, if user didn't define it.
    // so we are adding default and configuring queues concurrency.
    const orchestratorOptions =
      this.optsHandler.getOrchestratorInitializedOptions(
        context,
        this.orchestratorOptions(context) || {},
      );

    const orchestratorApi = this.createOrchestratorApi(
      /**
       * Driver factory function `DriverFactoryByDataSource`.
       */
      async (dataSource = 'default') => {
        if (driverPromise[dataSource]) {
          return driverPromise[dataSource];
        }

        // eslint-disable-next-line no-return-assign
        return driverPromise[dataSource] = (async () => {
          let driver: BaseDriver | null = null;

          try {
            driver = await this.resolveDriver(
              {
                ...context,
                dataSource,
              },
              orchestratorOptions,
            );

            if (typeof driver === 'object' && driver != null) {
              if (driver.setLogger) {
                driver.setLogger(this.logger);
              }

              await driver.testConnection();

              return driver;
            }

            throw new Error(
              `Unexpected return type, driverFactory must return driver (dataSource: "${dataSource}"), actual: ${getRealType(driver)}`
            );
          } catch (e) {
            driverPromise[dataSource] = null;

            if (driver) {
              await driver.release();
            }

            throw e;
          }
        })();
      },
      {
        externalDriverFactory: this.options.externalDriverFactory && (async () => {
          if (externalPreAggregationsDriverPromise) {
            return externalPreAggregationsDriverPromise;
          }

          // eslint-disable-next-line no-return-assign
          return externalPreAggregationsDriverPromise = (async () => {
            let driver: BaseDriver | null = null;

            try {
              driver = await this.options.externalDriverFactory(context);
              if (typeof driver === 'object' && driver != null) {
                if (driver.setLogger) {
                  driver.setLogger(this.logger);
                }

                await driver.testConnection();

                return driver;
              }

              throw new Error(
                `Unexpected return type, externalDriverFactory must return driver, actual: ${getRealType(driver)}`
              );
            } catch (e) {
              externalPreAggregationsDriverPromise = null;

              if (driver) {
                await driver.release();
              }

              throw e;
            }
          })();
        }),
        contextToDbType: async (dataSource) => contextToDbType({
          ...context,
          dataSource
        }),
        // speedup with cache
        contextToExternalDbType: () => externalDbType,
        redisPrefix: orchestratorId,
        skipExternalCacheAndQueue: externalDbType === 'cubestore',
        cacheAndQueueDriver: this.options.cacheAndQueueDriver,
        ...orchestratorOptions,
      }
    );

    this.orchestratorStorage.set(orchestratorId, orchestratorApi);

    return orchestratorApi;
  }

  protected createCompilerApi(repository, options: Record<string, any> = {}) {
    return new CompilerApi(
      repository,
      options.dbType || this.options.dbType,
      {
        schemaVersion: options.schemaVersion || this.options.schemaVersion,
        devServer: this.options.devServer,
        logger: this.logger,
        externalDbType: options.externalDbType,
        preAggregationsSchema: options.preAggregationsSchema,
        allowUngroupedWithoutPrimaryKey:
            this.options.allowUngroupedWithoutPrimaryKey ||
            getEnv('allowUngroupedWithoutPrimaryKey'),
        compileContext: options.context,
        dialectClass: options.dialectClass,
        externalDialectClass: options.externalDialectClass,
        allowJsDuplicatePropsInSchema: options.allowJsDuplicatePropsInSchema,
        sqlCache: this.options.sqlCache,
        standalone: this.standalone,
        allowNodeRequire: options.allowNodeRequire,
      },
    );
  }

  protected createOrchestratorApi(
    getDriver: DriverFactoryByDataSource,
    options: OrchestratorApiOptions
  ): OrchestratorApi {
    return new OrchestratorApi(
      getDriver,
      this.logger,
      options
    );
  }

  /**
   * @internal Please dont use this method directly, use refreshTimer
   */
  public handleScheduledRefreshInterval = async (options) => {
    const allContexts = await this.options.scheduledRefreshContexts();
    if (allContexts.length < 1) {
      this.logger('Refresh Scheduler Error', {
        error: 'At least one context should be returned by scheduledRefreshContexts'
      });
    }
    const contexts = allContexts.filter(
      (context) => this.contextAcceptor.shouldAccept(this.migrateBackgroundContext(context)).accepted
    );

    const batchLimit = pLimit(this.options.scheduledRefreshBatchSize);
    return Promise.all(
      contexts
        .map((context) => async () => {
          const queryingOptions: any = {
            ...options,
            concurrency: this.options.scheduledRefreshConcurrency,
          };

          if (this.options.scheduledRefreshTimeZones) {
            queryingOptions.timezones = this.options.scheduledRefreshTimeZones;
          }

          return this.runScheduledRefresh(context, queryingOptions);
        })
        // Limit the number of refresh contexts we process per iteration
        .map(batchLimit)
    );
  };

  protected getRefreshScheduler() {
    return new RefreshScheduler(this);
  }

  /**
   * @internal Please dont use this method directly, use refreshTimer
   */
  public async runScheduledRefresh(context: UserBackgroundContext | null, queryingOptions?: ScheduledRefreshOptions) {
    return this.getRefreshScheduler().runScheduledRefresh(
      this.migrateBackgroundContext(context),
      queryingOptions
    );
  }

  protected warningBackgroundContextShow: boolean = false;

  protected migrateBackgroundContext(ctx: UserBackgroundContext | null): RequestContext | null {
    let result: any = null;

    // We renamed authInfo to securityContext, but users can continue to use both ways
    if (ctx) {
      if (ctx.securityContext && !ctx.authInfo) {
        result = {
          ...ctx,
          authInfo: ctx.securityContext,
        };
      } else if (ctx.authInfo) {
        result = {
          ...ctx,
          securityContext: ctx.authInfo,
        };

        if (this.warningBackgroundContextShow) {
          this.logger('auth_info_deprecation', {
            warning: (
              'authInfo was renamed to securityContext, please migrate: ' +
              'https://github.com/cube-js/cube.js/blob/master/DEPRECATION.md#checkauthmiddleware'
            )
          });

          this.warningBackgroundContextShow = false;
        }
      }
    }

    return result;
  }

  /**
   * Returns driver instance by a given context
   */
  public async getDriver(
    context: DriverContext,
    options?: OrchestratorInitedOptions,
  ): Promise<BaseDriver> {
    // TODO (buntarb): this works fine without multiple data sources.
    if (!this.driver) {
      const driver = await this.resolveDriver(context, options);
      await driver.testConnection(); // TODO mutex
      this.driver = driver;
    }
    return this.driver;
  }

  /**
   * Resolve driver by the data source.
   */
  public async resolveDriver(
    context: DriverContext,
    options?: OrchestratorInitedOptions,
  ): Promise<BaseDriver> {
    const val = await this.options.driverFactory(context);
    if (isDriver(val)) {
      return <BaseDriver>val;
    } else {
      const { type, ...rest } = <DriverConfig>val;
      const opts = Object.keys(rest).length
        ? rest
        : {
          maxPoolSize:
            await CubejsServerCore.getDriverMaxPool(context, options),
          testConnectionTimeout: options?.testConnectionTimeout,
        };
      opts.dataSource = assertDataSource(context.dataSource);
      return CubejsServerCore.createDriver(type, opts);
    }
  }

  public async testConnections() {
    return this.orchestratorStorage.testConnections();
  }

  public async releaseConnections() {
    await this.orchestratorStorage.releaseConnections();

    if (this.maxCompilerCacheKeep) {
      clearInterval(this.maxCompilerCacheKeep);
    }

    if (this.scheduledRefreshTimerInterval) {
      await this.scheduledRefreshTimerInterval.cancel();
    }
  }

  public async beforeShutdown() {
    if (this.maxCompilerCacheKeep) {
      clearInterval(this.maxCompilerCacheKeep);
    }

    if (this.scheduledRefreshTimerInterval) {
      await this.scheduledRefreshTimerInterval.cancel(true);
    }
  }

  protected causeErrorPromise: Promise<any> | null = null;

  protected onUncaughtException = async (e: Error) => {
    console.error(e.stack || e);

    if (e.message && e.message.indexOf('Redis connection to') !== -1) {
      console.log('🛑 Cube.js Server requires locally running Redis instance to connect to');
      if (process.platform.indexOf('win') === 0) {
        console.log('💾 To install Redis on Windows please use https://github.com/MicrosoftArchive/redis/releases');
      } else if (process.platform.indexOf('darwin') === 0) {
        console.log('💾 To install Redis on Mac please use https://redis.io/topics/quickstart or `$ brew install redis`');
      } else {
        console.log('💾 To install Redis please use https://redis.io/topics/quickstart');
      }
    }

    if (!this.causeErrorPromise) {
      this.causeErrorPromise = this.event('Dev Server Fatal Error', {
        error: (e.stack || e.message || e).toString()
      });
    }

    await this.causeErrorPromise;

    process.exit(1);
  };

  public async shutdown() {
    if (this.devServer) {
      if (!process.env.CI) {
        process.removeListener('uncaughtException', this.onUncaughtException);
      }
    }

    if (this.apiGatewayInstance) {
      this.apiGatewayInstance.release();
    }

    return this.orchestratorStorage.releaseConnections();
  }
}
