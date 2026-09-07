/* eslint-disable global-require,no-return-assign */
import crypto from 'crypto';
import fs from 'fs-extra';
import { LRUCache } from 'lru-cache';
import isDocker from 'is-docker';
import pLimit from 'p-limit';

import {
  ApiGateway,
  ApiGatewayOptions,
  UserBackgroundContext
} from '@cubejs-backend/api-gateway';
import {
  CancelableInterval,
  createCancelableInterval,
  formatDuration,
  getEnv,
  assertDataSource,
  getRealType,
  hasPreAggregationsEnvVars,
  internalExceptions,
  track,
  FileRepository,
  SchemaFileRepository,
} from '@cubejs-backend/shared';

import type { Application as ExpressApplication } from 'express';

import { BaseDriver, DriverFactoryByDataSource, normalizeSqlPreamble } from '@cubejs-backend/query-orchestrator';
import type { SubscriptionServer, WebSocketSendMessageFn } from '@cubejs-backend/api-gateway';

import { RefreshScheduler, ScheduledRefreshOptions } from './RefreshScheduler';
import { OrchestratorApi, OrchestratorApiOptions } from './OrchestratorApi';
import { CompilerApi, type CompilerApiOptions } from './CompilerApi';
import { DevServer } from './DevServer';
import { agentCollect } from './agentCollect';
import { OrchestratorStorage } from './OrchestratorStorage';
import { createLogger } from './logger';
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
  DbTypeInternalFn,
  ExternalDbTypeFn,
  OrchestratorOptionsFn,
  OrchestratorInitedOptions,
  PreAggregationsSchemaFn,
  RequestContext,
  DriverContext,
  LoggerFn,
  DriverConfig,
  ScheduledRefreshTimeZonesFn,
  ContextToCubeStoreRouterIdFn,
  LoggerFnParams,
} from './types';
import {
  ContextToOrchestratorIdFn,
  ContextAcceptanceResult,
  ContextAcceptanceResultHttp,
  ContextAcceptanceResultWs,
  ContextAcceptor
} from './types';

const { version } = require('../../../package.json');

function wrapToFnIfNeeded<T, R>(possibleFn: T | ((a: R) => T)): (a: R) => T {
  if (typeof possibleFn === 'function') {
    return <any>possibleFn;
  }

  return () => possibleFn;
}

class AcceptAllAcceptor implements ContextAcceptor {
  public async shouldAccept(): Promise<ContextAcceptanceResult> {
    return { accepted: true };
  }

  public async shouldAcceptHttp(): Promise<ContextAcceptanceResultHttp> {
    return { accepted: true };
  }

  public async shouldAcceptWs(): Promise<ContextAcceptanceResultWs> {
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

  /**
   * Data sources already warned about an unapplied SQL preamble. Driver
   * resolution runs per data source and can retry, so this keeps the warning to
   * one line rather than one per attempt.
   */
  protected readonly sqlPreambleWarnedDataSources: Set<string> = new Set();

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  protected repositoryFactory: ((context: RequestContext) => SchemaFileRepository) | (() => FileRepository);

  protected contextToDbType: DbTypeInternalFn;

  protected contextToExternalDbType: ExternalDbTypeFn;

  protected compilerCache: LRUCache<string, CompilerApi>;

  protected readonly contextToOrchestratorId: ContextToOrchestratorIdFn;

  protected readonly contextToCubeStoreRouterId: ContextToCubeStoreRouterIdFn | null;

  protected readonly preAggregationsSchema: PreAggregationsSchemaFn;

  protected readonly scheduledRefreshTimeZones: ScheduledRefreshTimeZonesFn;

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

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  public readonly event: (name: string, props?: object) => Promise<void>;

  public projectFingerprint: string | null = null;

  public coreServerVersion: string | null = null;

  protected contextAcceptor: ContextAcceptor;

  public constructor(
    opts: CreateOptions = {},
    protected readonly systemOptions?: SystemOptions,
  ) {
    this.coreServerVersion = version;

    this.logger = opts.logger || createLogger(
      process.env.NODE_ENV === 'production',
      getEnv('logLevel'),
    );

    this.optsHandler = new OptsHandler(this, opts, systemOptions);
    this.options = this.optsHandler.getCoreInitializedOptions();

    this.repository = new FileRepository(this.options.schemaPath);
    this.repositoryFactory = this.options.repositoryFactory || (() => this.repository);

    this.contextToDbType = this.options.dbType;
    this.contextToExternalDbType = wrapToFnIfNeeded(this.options.externalDbType);
    this.preAggregationsSchema = wrapToFnIfNeeded(this.options.preAggregationsSchema);
    this.orchestratorOptions = wrapToFnIfNeeded(this.options.orchestratorOptions);
    this.scheduledRefreshTimeZones = wrapToFnIfNeeded(this.options.scheduledRefreshTimeZones || []);

    this.compilerCache = new LRUCache<string, CompilerApi>({
      max: this.options.compilerCacheSize || 250,
      ttl: this.options.maxCompilerCacheKeepAlive,
      updateAgeOnGet: this.options.updateCompilerCacheKeepAlive,
      // needed to clear the setInterval timer for proactive cache internal cleanups
      dispose: (v) => v.dispose(),
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
    this.contextToCubeStoreRouterId = this.options.contextToCubeStoreRouterId;

    // proactively free up old cache values occasionally
    if (this.options.maxCompilerCacheKeepAlive) {
      this.maxCompilerCacheKeep = setInterval(
        () => this.compilerCache.purgeStale(),
        this.options.maxCompilerCacheKeepAlive
      );
    }

    this.startScheduledRefreshTimer();

    this.event = async (event, props: LoggerFnParams) => {
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

      const internalExceptionsEnv = getEnv('internalExceptions');

      try {
        await track({
          timestamp: new Date().toJSON(),
          event,
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

      if (this.options.telemetry) {
        setInterval(() => {
          if (loadRequestCount > 0 || loadSqlRequestCount > 0) {
            this.event('Load Request Success Aggregated', { loadRequestSuccessCount: loadRequestCount, loadSqlRequestSuccessCount: loadSqlRequestCount });
          }
          loadRequestCount = 0;
          loadSqlRequestCount = 0;
        }, 60000);
      }

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
        // Filling timestamp as much as earlier as we can, otherwise it can be incorrect. Because next code is async
        // with await points which can be delayed with Node.js micro-tasking.
        params.timestamp = params.timestamp || new Date().toJSON();

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

  public initSubscriptionServer(sendMessage: WebSocketSendMessageFn): SubscriptionServer {
    const apiGateway = this.apiGateway();
    return apiGateway.initSubscriptionServer(sendMessage);
  }

  public initSQLServer() {
    const apiGateway = this.apiGateway();
    return apiGateway.getSQLServer();
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
        contextRejectionMiddleware: this.contextRejectionMiddleware.bind(this),
        wsContextAcceptor: this.contextAcceptor.shouldAcceptWs.bind(this.contextAcceptor),
        checkAuth: this.options.checkAuth,
        queryRewrite:
          this.options.queryRewrite || this.options.queryTransformer,
        extendContext: this.options.extendContext,
        playgroundAuthSecret: getEnv('playgroundAuthSecret'),
        apiSecrets: this.options.apiSecrets,
        jwt: this.options.jwt,
        refreshScheduler: this.getRefreshScheduler.bind(this),
        scheduledRefreshContexts: this.options.scheduledRefreshContexts,
        scheduledRefreshTimeZones: this.scheduledRefreshTimeZones,
        serverCoreVersion: this.coreServerVersion,
        contextToApiScopes: this.options.contextToApiScopes,
        gatewayPort: this.options.gatewayPort,
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
      const result = await this.contextAcceptor.shouldAcceptHttp(req.context);
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
          contextToGroups: this.options.contextToGroups,
          preAggregationsSchema: await this.preAggregationsSchema(context),
          context,
          allowJsDuplicatePropsInSchema: this.options.allowJsDuplicatePropsInSchema,
          allowNodeRequire: this.options.allowNodeRequire,
          fastReload: this.options.fastReload,
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
    this.compilerCache.clear();

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

    const contextToDbType: DbTypeInternalFn = this.contextToDbType.bind(this);
    const externalDbType = this.contextToExternalDbType(context);

    // orchestrator options can be empty, if user didn't define it.
    // so we are adding default and configuring queues concurrency.
    const orchestratorOptions =
      this.optsHandler.getOrchestratorInitializedOptions(
        context,
        (await this.orchestratorOptions(context)) || {},
      );

    const orchestratorApi = this.createOrchestratorApi(
      /**
       * Driver factory function `DriverFactoryByDataSource`.
       */
      async (dataSource = 'default', preAggregations = false) => {
        const factoryKey = preAggregations ? `${dataSource}@pre_agg` : dataSource;
        if (driverPromise[factoryKey]) {
          return driverPromise[factoryKey];
        }

        const hasSeparatePreAggEnv = hasPreAggregationsEnvVars(dataSource);
        const usePreAgg = preAggregations && hasSeparatePreAggEnv && !this.optsHandler.isCustomDriverFactory();

        // A pre-aggregation preamble that differs from the regular one needs its
        // own driver even when the credentials are shared. It is excluded from
        // `hasPreAggregationsEnvVars` on purpose — it is session setup, not a
        // connection target — so without this the two keys would share one
        // driver and a build would run the query-path preamble.
        //
        // Resolution is guarded: it reads the environment through
        // `assertDataSource`, which rejects a data source absent from
        // CUBEJS_DATASOURCES. That is already reported further down, by
        // `resolveDriver`, and pre-empting it here would replace a clear error
        // with one about the preamble.
        let hasSeparatePreAggSqlPreamble = false;
        try {
          // Normalized, so the sharing decision matches both the value the
          // driver runs and the pre-aggregation cache key. A re-indented or
          // trailing-newline value is the same preamble and must not split the
          // driver into a second connection pool.
          hasSeparatePreAggSqlPreamble =
            normalizeSqlPreamble(getEnv('dbSqlPreamble', { dataSource, preAggregations: true }))
            !== normalizeSqlPreamble(getEnv('dbSqlPreamble', { dataSource, preAggregations: false }));
        } catch (e) {
          hasSeparatePreAggSqlPreamble = false;
        }
        const shareDriverAcrossKeys = !preAggregations && !hasSeparatePreAggEnv && !hasSeparatePreAggSqlPreamble;

        if (preAggregations && hasSeparatePreAggEnv && this.optsHandler.isCustomDriverFactory()) {
          this.logger('Pre-aggregation driver conflict', {
            error: 'Both driverFactory and PRE_AGGREGATIONS env vars are defined. driverFactory will take precedence.',
            dataSource,
          });
        }

        driverPromise[factoryKey] = (async () => {
          let driver: BaseDriver | null = null;

          try {
            driver = await this.resolveDriver(
              {
                ...context,
                dataSource,
                preAggregations: usePreAgg || false,
                // Tracks "this driver serves pre-aggregation builds", which is
                // not the same question as `usePreAgg` ("resolve credentials
                // from the pre-aggregation namespace"). The SQL preamble is a
                // non-credential setting, so it deliberately does not switch
                // the credential namespace — without this flag a build that
                // set only a pre-aggregation preamble would silently run the
                // regular one, and the pre-aggregation would be keyed on a
                // preamble that never ran.
                preAggregationsSqlPreamble: preAggregations,
              },
              orchestratorOptions,
            );

            if (typeof driver === 'object' && driver != null) {
              if (driver.setLogger) {
                driver.setLogger(this.logger);
              }

              this.warnUnsupportedSqlPreamble(driver, dataSource);

              await driver.testConnection();

              return driver;
            }

            throw new Error(
              `Unexpected return type, driverFactory must return driver (dataSource: "${dataSource}"), actual: ${getRealType(driver)}`
            );
          } catch (e) {
            driverPromise[factoryKey] = null;

            if (shareDriverAcrossKeys) {
              driverPromise[`${dataSource}@pre_agg`] = null;
            }

            if (driver) {
              await driver.release();
            }

            throw e;
          }
        })();

        // No separate pre-agg driver needed — share the same promise for both keys
        if (shareDriverAcrossKeys) {
          driverPromise[`${dataSource}@pre_agg`] = driverPromise[factoryKey];
        }

        return driverPromise[factoryKey];
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
      this.createCompilerApiOptions(options),
    );
  }

  protected createCompilerApiOptions(options: Record<string, any> = {}): CompilerApiOptions {
    return {
      schemaVersion: options.schemaVersion || this.options.schemaVersion,
      contextToGroups: this.options.contextToGroups,
      devServer: this.options.devServer,
      logger: this.logger,
      externalDbType: options.externalDbType,
      preAggregationsSchema: options.preAggregationsSchema,
      allowUngroupedWithoutPrimaryKey:
          this.options.allowUngroupedWithoutPrimaryKey ||
          getEnv('allowUngroupedWithoutPrimaryKey'),
      convertTzForRawTimeDimension: getEnv('convertTzForRawTimeDimension'),
      localRefreshKey: getEnv('refreshKeyLocalTime'),
      compileContext: options.context,
      dialectClass: options.dialectClass,
      externalDialectClass: options.externalDialectClass,
      allowJsDuplicatePropsInSchema: options.allowJsDuplicatePropsInSchema,
      sqlCache: this.options.sqlCache,
      standalone: this.standalone,
      allowNodeRequire: options.allowNodeRequire,
      fastReload: options.fastReload || getEnv('fastReload'),
      compilerCacheSize: this.options.compilerCacheSize || 250,
      maxCompilerCacheKeepAlive: this.options.maxCompilerCacheKeepAlive,
      updateCompilerCacheKeepAlive: this.options.updateCompilerCacheKeepAlive,
    };
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
   * @internal Please don't use this method directly, use refreshTimer
   */
  public handleScheduledRefreshInterval = async (options) => {
    const allContexts = await this.options.scheduledRefreshContexts();
    if (allContexts.length < 1) {
      this.logger('Refresh Scheduler Error', {
        error: 'At least one context should be returned by scheduledRefreshContexts'
      });
    }

    const contexts = [];

    for (const allContext of allContexts) {
      const resContext = this.migrateBackgroundContext(allContext);
      const res = await this.contextAcceptor.shouldAccept(resContext);

      if (res.accepted) {
        contexts.push(resContext || {});
      }
    }

    const batchLimit = pLimit(this.options.scheduledRefreshBatchSize);
    return Promise.all(
      contexts
        .map((context) => async () => {
          const queryingOptions: any = {
            ...options,
            concurrency: this.options.scheduledRefreshConcurrency,
          };

          const timezonesFromOptionsOrSecurityContext = await this.scheduledRefreshTimeZones(context);
          if (timezonesFromOptionsOrSecurityContext.length > 0) {
            queryingOptions.timezones = timezonesFromOptionsOrSecurityContext;
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
   * @internal Please don't use this method directly, use refreshTimer
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
   * Warns once per data source when a SQL preamble is configured for a driver
   * that does not apply it.
   *
   * The option is then a no-op for queries, but it still participates in the
   * pre-aggregation version key, so setting it rebuilds every pre-aggregation on
   * that data source for no behavioural change. Cost with no effect, and
   * previously with no signal either.
   *
   * Asks the driver rather than consulting a list of dbTypes: `supportsSqlPreamble()`
   * is inherited, so `RedshiftDriver extends PostgresDriver` and every JDBC-based
   * driver answer correctly without being enumerated anywhere. It also fires here
   * rather than in the orchestrator, so a deployment with no pre-aggregations at
   * all still sees it — the query-path no-op is the part that surprises people.
   */
  protected warnUnsupportedSqlPreamble(driver: BaseDriver, dataSource: string) {
    if (this.sqlPreambleWarnedDataSources.has(dataSource)) {
      return;
    }

    // Optional on `DriverInterface`, so a driver that does not extend
    // `BaseDriver` — or was compiled against an older one — is not assumed
    // unsupported. Saying nothing beats telling someone their working config
    // does nothing.
    if (typeof driver.supportsSqlPreamble !== 'function' || driver.supportsSqlPreamble()) {
      return;
    }

    let configured: string | undefined;
    try {
      configured = normalizeSqlPreamble(getEnv('dbSqlPreamble', { dataSource }))
        ?? normalizeSqlPreamble(getEnv('dbSqlPreamble', { dataSource, preAggregations: true }));
    } catch (e) {
      // An undeclared data source is reported elsewhere, with a clearer message.
      return;
    }

    if (configured) {
      this.sqlPreambleWarnedDataSources.add(dataSource);
      this.logger('SQL preamble not applied', {
        warning:
          'A SQL preamble is configured for this data source, but its driver does not apply it, so ' +
          'it changes nothing about how queries run. It still participates in the pre-aggregation ' +
          'version key, so setting it rebuilds every pre-aggregation on this data source. Supported ' +
          'by the BigQuery, Snowflake, Postgres, Redshift, CrateDB, Materialize, MySQL, DuckDB and ' +
          'JDBC-based drivers.',
        dataSource,
      });
    }
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
      opts.preAggregations = context.preAggregations || false;
      opts.preAggregationsSqlPreamble =
        context.preAggregationsSqlPreamble ?? context.preAggregations ?? false;
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

    this.compilerCache.clear();

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
    this.compilerCache.clear();

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
