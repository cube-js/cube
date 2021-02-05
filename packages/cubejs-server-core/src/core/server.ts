/* eslint-disable global-require */
import crypto from 'crypto';
import fs from 'fs-extra';
import path from 'path';
import LRUCache from 'lru-cache';
import isDocker from 'is-docker';

import { ApiGateway } from '@cubejs-backend/api-gateway';
import {
  CancelableInterval,
  createCancelableInterval, formatDuration,
  getAnonymousId,
  getEnv,
  internalExceptions,
  track,
} from '@cubejs-backend/shared';
import type { Application as ExpressApplication } from 'express';
import type { BaseDriver } from '@cubejs-backend/query-orchestrator';
import type {
  ContextToAppIdFn,
  CreateOptions,
  DatabaseType,
  DbTypeFn,
  ExternalDbTypeFn,
  OrchestratorOptionsFn,
  PreAggregationsSchemaFn,
  RequestContext,
  DriverContext,
  SchemaFileRepository,
  UserBackgroundContext,
} from './types';

import { FileRepository } from './FileRepository';
import { RefreshScheduler } from './RefreshScheduler';
import { OrchestratorApi } from './OrchestratorApi';
import { CompilerApi } from './CompilerApi';
import { DevServer } from './DevServer';
import agentCollect from './agentCollect';
import { OrchestratorStorage } from './OrchestratorStorage';
import { prodLogger, devLogger } from './logger';

import DriverDependencies from './DriverDependencies';
import optionsValidate from './optionsValidate';

const { version } = require('../../../package.json');

const checkEnvForPlaceholders = () => {
  const placeholderSubstr = '<YOUR_DB_';
  const credentials = [
    'CUBEJS_DB_HOST',
    'CUBEJS_DB_NAME',
    'CUBEJS_DB_USER',
    'CUBEJS_DB_PASS'
  ];
  if (
    credentials.find((credential) => (
      process.env[credential] && process.env[credential].indexOf(placeholderSubstr) === 0
    ))
  ) {
    throw new Error('Your .env file contains placeholders in DB credentials. Please replace them with your DB credentials.');
  }
};

type RequireOne<T, K extends keyof T> = {
  [X in Exclude<keyof T, K>]?: T[X]
} & {
  [P in K]-?: T[P]
}

export type ServerCoreInitializedOptions = RequireOne<
  CreateOptions,
  // This fields are required, because we add default values in constructor
  'dbType' | 'apiSecret' | 'devServer' | 'telemetry' | 'logger' | 'dashboardAppPath' | 'dashboardAppPort' |
  'driverFactory' | 'dialectFactory' |
  'externalDriverFactory' | 'externalDialectFactory' |
  'scheduledRefreshContexts'
>;

function wrapToFnIfNeeded<T, R>(possibleFn: T|((a: R) => T)): (a: R) => T {
  if (typeof possibleFn === 'function') {
    return <any>possibleFn;
  }

  return () => possibleFn;
}

export class CubejsServerCore {
  public readonly repository: FileRepository;

  protected devServer: DevServer|undefined;

  protected readonly orchestratorStorage: OrchestratorStorage = new OrchestratorStorage();

  protected readonly repositoryFactory: ((context: RequestContext) => SchemaFileRepository) | (() => FileRepository);

  protected readonly contextToDbType: DbTypeFn;

  protected contextToExternalDbType: ExternalDbTypeFn;

  protected compilerCache: LRUCache<string, CompilerApi>;

  protected contextToOrchestratorId: any;

  protected readonly preAggregationsSchema: PreAggregationsSchemaFn;

  protected readonly orchestratorOptions: OrchestratorOptionsFn;

  public logger: (type: string, params: Record<string, any>) => void;

  protected preAgentLogger: any;

  protected readonly options: ServerCoreInitializedOptions;

  protected readonly contextToAppId: ContextToAppIdFn = () => process.env.CUBEJS_APP || 'STANDALONE';

  protected readonly standalone: boolean = true;

  protected maxCompilerCacheKeep: NodeJS.Timeout|null = null;

  protected scheduledRefreshTimerInterval: CancelableInterval|null = null;

  protected driver: BaseDriver|null = null;

  protected apiGatewayInstance: ApiGateway|null = null;

  public readonly event: (name: string, props?: object) => Promise<void>;

  public projectFingerprint: string|null = null;

  public anonymousId: string|null = null;

  public coreServerVersion: string|null = null;

  public constructor(opts: CreateOptions = {}) {
    this.options = this.handleConfiguration(opts);

    if (
      !this.options.devServer || (this.options.devServer && this.configFileExists())
    ) {
      if (
        !this.options.driverFactory ||
        !this.options.apiSecret ||
        !this.options.dbType
      ) {
        throw new Error('driverFactory, apiSecret, dbType are required options');
      }
    }

    this.logger = this.options.logger;
    this.repository = new FileRepository(this.options.schemaPath);
    this.repositoryFactory = this.options.repositoryFactory || (() => this.repository);

    this.contextToDbType = wrapToFnIfNeeded(this.options.dbType);
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

    if (this.options.contextToDataSourceId) {
      throw new Error('contextToDataSourceId has been deprecated and removed. Use contextToOrchestratorId instead.');
    }

    this.contextToOrchestratorId = this.options.contextToOrchestratorId || this.contextToAppId;

    // proactively free up old cache values occasionally
    if (this.options.maxCompilerCacheKeepAlive) {
      this.maxCompilerCacheKeep = setInterval(
        () => this.compilerCache.prune(),
        this.options.maxCompilerCacheKeepAlive
      );
    }

    const scheduledRefreshTimer = this.detectScheduledRefreshTimer(
      this.options.scheduledRefreshTimer || getEnv('refreshTimer') || getEnv('scheduledRefresh')
    );
    if (scheduledRefreshTimer) {
      this.scheduledRefreshTimerInterval = createCancelableInterval(
        this.handleScheduledRefreshInterval,
        {
          interval: scheduledRefreshTimer,
          onDuplicatedExecution: (intervalId) => this.logger('Refresh Scheduler Interval Error', {
            error: `Previous interval #${intervalId} was not finished with ${scheduledRefreshTimer} interval`
          }),
          onDuplicatedStateResolved: (intervalId, elapsed) => this.logger('Refresh Scheduler Long Execution', {
            warning: `Interval #${intervalId} finished after ${formatDuration(elapsed)}`
          })
        }
      );
    }

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
          internalExceptions(e);
        }
      }

      if (!this.anonymousId) {
        this.anonymousId = getAnonymousId();
      }

      if (!this.coreServerVersion) {
        this.coreServerVersion = version;
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
        internalExceptions(e);
      }
    };

    this.initAgent();

    if (this.options.devServer && !this.configFileExists()) {
      this.event('first_server_start');
    }

    if (this.options.devServer) {
      this.devServer = new DevServer(this);
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
          msg === 'Slow Query Warning'
        ) {
          this.event(msg, { error: params.error });
        }
        oldLogger(msg, params);
      });

      if (!process.env.CI) {
        process.on('uncaughtException', this.onUncaughtException);
      }
    } else {
      const oldLogger = this.logger;
      let loadRequestCount = 0;

      this.logger = ((msg, params) => {
        if (msg === 'Load Request Success') {
          loadRequestCount++;
        }
        oldLogger(msg, params);
      });

      setInterval(() => {
        this.event('Load Request Success Aggregated', { loadRequestSuccessCount: loadRequestCount });
        loadRequestCount = 0;
      }, 60000);

      this.event('Server Start');
    }
  }

  protected handleConfiguration(opts: CreateOptions): ServerCoreInitializedOptions {
    optionsValidate(opts);

    const dbType = opts.dbType || <DatabaseType|undefined>process.env.CUBEJS_DB_TYPE;
    const externalDbType = opts.externalDbType || <DatabaseType|undefined>process.env.CUBEJS_EXT_DB_TYPE;
    const devServer = process.env.NODE_ENV !== 'production' || process.env.CUBEJS_DEV_MODE === 'true';

    const options: ServerCoreInitializedOptions = {
      dbType,
      externalDbType,
      devServer,
      driverFactory: () => typeof dbType === 'string' && CubejsServerCore.createDriver(dbType),
      dialectFactory: (ctx) => CubejsServerCore.lookupDriverClass(ctx.dbType).dialectClass &&
        CubejsServerCore.lookupDriverClass(ctx.dbType).dialectClass(),
      externalDriverFactory: externalDbType && (
        () => new (CubejsServerCore.lookupDriverClass(externalDbType))({
          host: process.env.CUBEJS_EXT_DB_HOST,
          database: process.env.CUBEJS_EXT_DB_NAME,
          port: process.env.CUBEJS_EXT_DB_PORT,
          user: process.env.CUBEJS_EXT_DB_USER,
          password: process.env.CUBEJS_EXT_DB_PASS,
        })
      ),
      externalDialectFactory: () => typeof externalDbType === 'string' &&
        CubejsServerCore.lookupDriverClass(externalDbType).dialectClass &&
        CubejsServerCore.lookupDriverClass(externalDbType).dialectClass(),
      apiSecret: process.env.CUBEJS_API_SECRET,
      telemetry: process.env.CUBEJS_TELEMETRY !== 'false',
      scheduledRefreshTimeZones: process.env.CUBEJS_SCHEDULED_REFRESH_TIMEZONES &&
        process.env.CUBEJS_SCHEDULED_REFRESH_TIMEZONES.split(',').map(t => t.trim()),
      scheduledRefreshContexts: async () => [null],
      basePath: '/cubejs-api',
      dashboardAppPath: 'dashboard-app',
      dashboardAppPort: 3000,
      scheduledRefreshConcurrency: parseInt(process.env.CUBEJS_SCHEDULED_REFRESH_CONCURRENCY, 10),
      preAggregationsSchema: getEnv('preAggregationsSchema') || (
        devServer ? 'dev_pre_aggregations' : 'prod_pre_aggregations'
      ),
      schemaPath: process.env.CUBEJS_SCHEMA_PATH || 'schema',
      logger: opts.logger || process.env.NODE_ENV !== 'production'
        ? devLogger(process.env.CUBEJS_LOG_LEVEL)
        : prodLogger(process.env.CUBEJS_LOG_LEVEL),
      ...opts,
    };

    if (opts.contextToAppId && !opts.scheduledRefreshContexts) {
      options.logger('Multitenancy Without ScheduledRefreshContexts', {
        warning: (
          'You are using multitenancy without configuring scheduledRefreshContexts, which can lead to issues where the ' +
          'security context will be undefined while Cube.js will do background refreshing: ' +
          'https://cube.dev/docs/config#options-reference-scheduled-refresh-contexts'
        ),
      });
    }

    return options;
  }

  public configFileExists(): boolean {
    return (fs.existsSync('./.env') || fs.existsSync('./cube.js'));
  }

  protected detectScheduledRefreshTimer(scheduledRefreshTimer?: string | number | boolean): number|null {
    if (scheduledRefreshTimer && (
      typeof scheduledRefreshTimer === 'number' ||
      typeof scheduledRefreshTimer === 'string' && scheduledRefreshTimer.match(/^\d+$/)
    )) {
      scheduledRefreshTimer = parseInt(<any>scheduledRefreshTimer, 10) * 1000;
    }

    if (scheduledRefreshTimer && typeof scheduledRefreshTimer === 'string') {
      scheduledRefreshTimer = scheduledRefreshTimer.toLowerCase() === 'true';
    }

    if (scheduledRefreshTimer == null) {
      scheduledRefreshTimer = process.env.NODE_ENV !== 'production';
    }

    if (typeof scheduledRefreshTimer === 'boolean' && scheduledRefreshTimer) {
      scheduledRefreshTimer = 30000;
    }

    return <any>scheduledRefreshTimer;
  }

  protected initAgent() {
    if (process.env.CUBEJS_AGENT_ENDPOINT_URL) {
      const oldLogger = this.logger;
      this.preAgentLogger = oldLogger;
      this.logger = (msg, params) => {
        oldLogger(msg, params);
        agentCollect(
          {
            msg,
            ...params
          },
          process.env.CUBEJS_AGENT_ENDPOINT_URL,
          oldLogger
        );
      };
    }
  }

  protected async flushAgent() {
    if (process.env.CUBEJS_AGENT_ENDPOINT_URL) {
      await agentCollect(
        { msg: 'Flush Agent' },
        process.env.CUBEJS_AGENT_ENDPOINT_URL,
        this.preAgentLogger
      );
    }
  }

  public static create(options?: CreateOptions) {
    return new CubejsServerCore(options);
  }

  public async initApp(app: ExpressApplication) {
    checkEnvForPlaceholders();

    const apiGateway = this.apiGateway();
    apiGateway.initApp(app);

    if (this.options.devServer) {
      this.devServer.initDevEnv(app, this.options);
    } else {
      app.get('/', (req, res) => {
        res.status(200)
          .send('<html><body>Cube.js server is running in production mode. <a href="https://cube.dev/docs/deployment#production-mode">Learn more about production mode</a>.</body></html>');
      });
    }
  }

  public initSubscriptionServer(sendMessage) {
    checkEnvForPlaceholders();

    const apiGateway = this.apiGateway();
    return apiGateway.initSubscriptionServer(sendMessage);
  }

  protected apiGateway() {
    if (!this.apiGatewayInstance) {
      this.apiGatewayInstance = new ApiGateway(
        this.options.apiSecret,
        this.getCompilerApi.bind(this),
        this.getOrchestratorApi.bind(this),
        this.logger, {
          standalone: this.standalone,
          dataSourceStorage: this.orchestratorStorage,
          basePath: this.options.basePath,
          checkAuthMiddleware: this.options.checkAuthMiddleware,
          checkAuth: this.options.checkAuth,
          queryTransformer: this.options.queryTransformer,
          extendContext: this.options.extendContext,
          refreshScheduler: () => new RefreshScheduler(this),
        }
      );
    }

    return this.apiGatewayInstance;
  }

  public getCompilerApi(context: RequestContext) {
    const appId = this.contextToAppId(context);
    let compilerApi = this.compilerCache.get(appId);
    const currentSchemaVersion = this.options.schemaVersion && (() => this.options.schemaVersion(context));

    if (!compilerApi) {
      compilerApi = this.createCompilerApi(
        this.repositoryFactory(context), {
          dbType: (dataSourceContext) => this.contextToDbType({ ...context, ...dataSourceContext }),
          externalDbType: this.contextToExternalDbType(context),
          dialectClass: (dialectContext) => this.options.dialectFactory &&
            this.options.dialectFactory({ ...context, ...dialectContext }),
          externalDialectClass: this.options.externalDialectFactory && this.options.externalDialectFactory(context),
          schemaVersion: currentSchemaVersion,
          preAggregationsSchema: this.preAggregationsSchema(context),
          context,
          allowJsDuplicatePropsInSchema: this.options.allowJsDuplicatePropsInSchema
        }
      );

      this.compilerCache.set(appId, compilerApi);
    }

    compilerApi.schemaVersion = currentSchemaVersion;
    return compilerApi;
  }

  public getOrchestratorApi(context: DriverContext): OrchestratorApi {
    const orchestratorId = this.contextToOrchestratorId(context);

    if (this.orchestratorStorage.has(orchestratorId)) {
      return this.orchestratorStorage.get(orchestratorId);
    }

    const driverPromise = {};
    let externalPreAggregationsDriverPromise;

    const orchestratorApi = this.createOrchestratorApi({
      getDriver: async (dataSource) => {
        if (!driverPromise[dataSource || 'default']) {
          orchestratorApi.addDataSeenSource(dataSource);
          const driver = await this.options.driverFactory({ ...context, dataSource });
          if (driver.setLogger) {
            driver.setLogger(this.logger);
          }
          driverPromise[dataSource || 'default'] = driver.testConnection().then(() => driver).catch(e => {
            driverPromise[dataSource || 'default'] = null;
            throw e;
          });
        }
        return driverPromise[dataSource || 'default'];
      },
      getExternalDriverFactory: this.options.externalDriverFactory && (async () => {
        if (!externalPreAggregationsDriverPromise) {
          const driver = await this.options.externalDriverFactory(context);
          if (driver.setLogger) {
            driver.setLogger(this.logger);
          }
          externalPreAggregationsDriverPromise = driver.testConnection().then(() => driver).catch(e => {
            externalPreAggregationsDriverPromise = null;
            throw e;
          });
        }
        return externalPreAggregationsDriverPromise;
      }),
      redisPrefix: orchestratorId,
      orchestratorOptions: this.orchestratorOptions(context)
    });

    this.orchestratorStorage.set(orchestratorId, orchestratorApi);

    return orchestratorApi;
  }

  protected createCompilerApi(repository, options) {
    options = options || {};
    return new CompilerApi(repository, options.dbType || this.options.dbType, {
      schemaVersion: options.schemaVersion || this.options.schemaVersion,
      devServer: this.options.devServer,
      logger: this.logger,
      externalDbType: options.externalDbType,
      preAggregationsSchema: options.preAggregationsSchema,
      allowUngroupedWithoutPrimaryKey: this.options.allowUngroupedWithoutPrimaryKey,
      compileContext: options.context,
      dialectClass: options.dialectClass,
      externalDialectClass: options.externalDialectClass,
      allowJsDuplicatePropsInSchema: options.allowJsDuplicatePropsInSchema
    });
  }

  protected createOrchestratorApi(options: any = {}): OrchestratorApi {
    return new OrchestratorApi(options.getDriver || this.getDriver.bind(this), this.logger, {
      redisPrefix: options.redisPrefix || process.env.CUBEJS_APP,
      externalDriverFactory: options.getExternalDriverFactory,
      ...(options.orchestratorOptions || this.options.orchestratorOptions)
    });
  }

  /**
   * @internal Please dont use this method directly, use refreshTimer
   */
  public handleScheduledRefreshInterval = async () => {
    const contexts = await this.options.scheduledRefreshContexts();
    if (contexts.length < 1) {
      this.logger('Refresh Scheduler Error', {
        error: 'At least one context should be returned by scheduledRefreshContexts'
      });
    }

    return Promise.all(contexts.map(async context => {
      const queryingOptions: any = { concurrency: this.options.scheduledRefreshConcurrency };

      if (this.options.scheduledRefreshTimeZones) {
        queryingOptions.timezones = this.options.scheduledRefreshTimeZones;
      }

      await this.runScheduledRefresh(context, queryingOptions);
    }));
  }

  protected getRefreshScheduler() {
    return new RefreshScheduler(this);
  }

  /**
   * @internal Please dont use this method directly, use refreshTimer
   */
  public async runScheduledRefresh(context: UserBackgroundContext|null, queryingOptions?: any) {
    return this.getRefreshScheduler().runScheduledRefresh(this.migrateBackgroundContext(context), queryingOptions);
  }

  protected warningBackgroundContextShow: boolean = false;

  protected migrateBackgroundContext(ctx: UserBackgroundContext | null): RequestContext|null {
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

  public async getDriver(ctx: DriverContext) {
    if (!this.driver) {
      const driver = await this.options.driverFactory(ctx);
      await driver.testConnection(); // TODO mutex
      this.driver = driver;
    }

    return this.driver;
  }

  public static createDriver(dbType: DatabaseType) {
    checkEnvForPlaceholders();

    const module = CubejsServerCore.lookupDriverClass(dbType);
    if (module.default) {
      // eslint-disable-next-line new-cap
      return new module.default();
    }

    // eslint-disable-next-line new-cap
    return new module();
  }

  protected static lookupDriverClass(dbType) {
    // eslint-disable-next-line global-require,import/no-dynamic-require
    const module = require(CubejsServerCore.driverDependencies(dbType || process.env.CUBEJS_DB_TYPE));
    if (module.default) {
      return module.default;
    }

    return module;
  }

  public static driverDependencies(dbType: DatabaseType) {
    if (DriverDependencies[dbType]) {
      return DriverDependencies[dbType];
    } else if (fs.existsSync(path.join('node_modules', `${dbType}-cubejs-driver`))) {
      return `${dbType}-cubejs-driver`;
    }

    throw new Error(`Unsupported db type: ${dbType}`);
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

  protected causeErrorPromise: Promise<any>|null = null;

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
  }

  public async shutdown() {
    if (this.devServer) {
      if (!process.env.CI) {
        process.removeListener('uncaughtException', this.onUncaughtException);
      }
    }

    return this.orchestratorStorage.releaseConnections();
  }

  public static version() {
    return version;
  }
}
