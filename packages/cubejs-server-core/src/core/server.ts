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

import { BaseDriver, DriverFactoryByDataSource } from '@cubejs-backend/query-orchestrator';
import type { SubscriptionServer, WebSocketSendMessageFn } from '@cubejs-backend/api-gateway';

import { RefreshScheduler, ScheduledRefreshOptions } from './RefreshScheduler';
import { OrchestratorApi, OrchestratorApiOptions } from './OrchestratorApi';
import { CompilerApi, type CompilerApiOptions } from './CompilerApi';
import { DevServer } from './DevServer';
import { agentCollect } from './agentCollect';
import { OrchestratorStorage } from './OrchestratorStorage';
import { createLogger } from './logger';
import { OptsHandler } from './OptsHandler';
import { fingerprint } from './driver-config-fingerprint';
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

/**
 * Rebuilds of one data source's driver before the log escalates to naming a
 * likely misconfiguration. A rotating credential rebuilds a few times a day, so
 * reaching this within a process means contexts are displacing each other.
 */
const DRIVER_REBUILD_WARN_THRESHOLD = 50;

/**
 * How many times one request will retry after losing the race to rebuild a
 * driver before settling for whatever is cached. Bounds the work a single
 * request can be made to do when contexts keep displacing each other's driver.
 */
const MAX_DRIVER_REBUILD_ATTEMPTS = 3;

/**
 * What a cached driver was built from. `null` on either field means "cannot
 * tell whether it changed", which is always read as "assume it did not".
 */
type DriverOrigin = {
  securityContextFingerprint: string | null;
  configFingerprint: string | null;
};

/** A `driverFactory` result together with the context that produced it. */
type DriverFactoryResult = {
  value: DriverConfig | BaseDriver;
  securityContextFingerprint: string | null;
};

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
   * The request context each cached orchestrator most recently served.
   *
   * An orchestrator's driver factory closes over the context of the request
   * that created it, and the driver it resolves is then cached for the life of
   * the process. When that driver's configuration is derived from the context —
   * a per-user OAuth token, say — it goes stale the moment the credential
   * rotates. Tracking the latest context lets the factory notice. Keyed by the
   * api instance so an entry disappears with the orchestrator it belongs to.
   */
  protected readonly orchestratorRequestContexts =
    new WeakMap<OrchestratorApi, { current: RequestContext }>();

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
      const cachedOrchestratorApi = this.orchestratorStorage.get(orchestratorId);
      const cachedContextRef = this.orchestratorRequestContexts.get(cachedOrchestratorApi);

      // Keep the driver factory's view of the request context current. Without
      // this it stays pinned to whichever request happened to create the
      // orchestrator, and a driver built from context-derived credentials can
      // never be rebuilt when they rotate.
      if (cachedContextRef) {
        cachedContextRef.current = context;
      }

      return cachedOrchestratorApi;
    }

    const requestContextRef: { current: RequestContext } = { current: context };

    /**
     * Hash table to store promises which will be resolved with the
     * datasource drivers. DriverFactoryByDataSource function is closure
     * this constant.
     */
    const driverPromise: Record<string, Promise<BaseDriver>> = {};

    /**
     * What each cached driver in `driverPromise` was built from, so a changed
     * configuration can be detected. Keyed identically to `driverPromise`.
     */
    const driverOrigin: Record<string, DriverOrigin> = {};

    /**
     * How many times each key has been rebuilt. Reported with the rebuild so a
     * deployment whose `contextToOrchestratorId` does not partition by whatever
     * `driverFactory` reads — every user sharing one orchestrator, say — is
     * diagnosable: it rebuilds on request after request rather than once per
     * credential rotation.
     */
    const driverRebuilds: Record<string, number> = {};

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

    /**
     * Driver factory function `DriverFactoryByDataSource`. Named so the rebuild
     * path can re-enter it when another caller wins the race to replace a key.
     */
    const resolveDataSourceDriver = async (
      dataSource = 'default',
      preAggregations = false,
      attempt = 0,
    ): Promise<BaseDriver> => {
      const factoryKey = preAggregations ? `${dataSource}@pre_agg` : dataSource;

      const hasSeparatePreAggEnv = hasPreAggregationsEnvVars(dataSource);
      const usePreAgg = preAggregations && hasSeparatePreAggEnv && !this.optsHandler.isCustomDriverFactory();

      const driverContext = (): DriverContext => ({
        ...requestContextRef.current,
        dataSource,
        preAggregations: usePreAgg || false,
      });

      /**
       * Every key that resolves to the one driver built here. Without separate
       * pre-aggregation credentials `usePreAgg` is false whichever key was
       * asked for, so both describe an identically configured driver and share
       * a single instance — they must therefore be written, and invalidated,
       * together. Doing it per requested key instead lets the two diverge into
       * two pools where the deployment expects one.
       */
      const aliasedKeys = hasSeparatePreAggEnv
        ? [factoryKey]
        : [dataSource, `${dataSource}@pre_agg`];

      const invalidate = () => aliasedKeys.forEach((key) => {
        driverPromise[key] = null;
        delete driverOrigin[key];
      });

      // Already resolved by the staleness check below, so the factory is not
      // asked twice for the same rebuild.
      let resolvedFactoryResult: DriverFactoryResult | undefined;

      const cached = driverPromise[factoryKey];

      if (cached) {
        const staleness = await this.resolveDriverStaleness(
          driverOrigin[factoryKey],
          driverContext(),
        );

        // `resolveDriverStaleness` awaits the user's factory, so another caller
        // may have replaced or invalidated this key in the meantime. Its work
        // supersedes ours: start over rather than release a driver it has
        // already handed out, or build a second pool alongside it.
        if (driverPromise[factoryKey] !== cached) {
          const superseding = driverPromise[factoryKey];

          // Retry, so this request ends up on a driver matching its own
          // context — but bounded. Where contexts keep displacing each other
          // this request could otherwise lose every round and pay for a
          // user-supplied factory call each time. Past the bound, take what is
          // cached: degrading to a reused driver is this design's fallback
          // everywhere else, and it is strictly better than starving.
          if (attempt < MAX_DRIVER_REBUILD_ATTEMPTS) {
            return resolveDataSourceDriver(dataSource, preAggregations, attempt + 1);
          }

          if (superseding) {
            return superseding;
          }

          // Invalidated rather than replaced, so there is nothing to reuse —
          // fall through and build, which cannot recurse again.
        }

        if (!staleness.stale) {
          return cached;
        }

        // Counted per alias set, not per key: a rotation seen first through
        // `default@pre_agg` and then through `default` is one rebuild of one
        // shared driver, and must not read as two counters at 1.
        const rebuildKey = aliasedKeys[0];
        driverRebuilds[rebuildKey] = (driverRebuilds[rebuildKey] || 0) + 1;
        const rebuildCount = driverRebuilds[rebuildKey];

        // Carries `warning` so it survives the default log level: a plain-params
        // message matches no allowlist in `prodLogger`/`devLogger` and is
        // dropped below `trace`. Tearing down a connection pool is an event an
        // operator needs to be able to correlate against, and the threshold
        // message below arrives too late to reconstruct the first rebuilds.
        this.logger('Rebuilding driver on configuration change', {
          dataSource,
          preAggregations,
          rebuildCount,
          warning: 'Driver configuration changed; replacing the connection.',
        });

        // A credential rotation rebuilds a handful of times a day. Rebuilding
        // this often means the orchestrator id does not partition by whatever
        // the factory reads, so contexts that need different connections keep
        // displacing each other's driver.
        if (rebuildCount === DRIVER_REBUILD_WARN_THRESHOLD) {
          this.logger('Driver rebuilt repeatedly', {
            dataSource,
            rebuildCount,
            warning: 'Driver configuration keeps changing for one orchestrator. '
              + 'contextToOrchestratorId likely does not distinguish the contexts '
              + 'driverFactory returns different connections for.',
          });
        }

        // Clear every key pointing at the replaced driver, not just the one
        // asked for: a surviving alias would keep handing out a driver whose
        // pool is being drained, and would release it a second time when it
        // was itself found stale.
        Object.keys(driverPromise)
          .filter((key) => driverPromise[key] === cached)
          .forEach((key) => {
            driverPromise[key] = null;
            delete driverOrigin[key];
          });

        // Graceful: `release` drains the pool, so queries already running on
        // the replaced driver finish before its connections are closed. It is
        // deliberately not awaited — this request should not wait on the
        // previous driver's in-flight work — and its failure must not fail
        // this request.
        cached
          .then((driver) => driver.release())
          .catch((error) => this.logger('Driver release error', {
            dataSource,
            error: (error as Error).stack || (error as Error).toString(),
          }));

        resolvedFactoryResult = staleness.factoryResult;
      }

      if (preAggregations && hasSeparatePreAggEnv && this.optsHandler.isCustomDriverFactory()) {
        this.logger('Pre-aggregation driver conflict', {
          error: 'Both driverFactory and PRE_AGGREGATIONS env vars are defined. driverFactory will take precedence.',
          dataSource,
        });
      }

      // Shared by reference across `aliasedKeys`, so every key describes the
      // one driver they all resolve to. Starts empty: until the factory has
      // been called there is nothing to compare against, and
      // `resolveDriverStaleness` reads that as "reuse".
      const origin: DriverOrigin = {
        securityContextFingerprint: null,
        configFingerprint: null,
      };

      aliasedKeys.forEach((key) => {
        driverOrigin[key] = origin;
      });

      const pending = (async () => {
        let driver: BaseDriver | null = null;

        try {
          const currentDriverContext = driverContext();
          const factoryResult = resolvedFactoryResult ?? {
            value: await this.options.driverFactory(currentDriverContext),
            securityContextFingerprint: fingerprint(currentDriverContext.securityContext),
          };

          origin.securityContextFingerprint = factoryResult.securityContextFingerprint;
          origin.configFingerprint = isDriver(factoryResult.value)
            ? null
            : fingerprint(factoryResult.value);

          driver = await this.createDriverFromFactoryResult(
            factoryResult.value,
            currentDriverContext,
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
          // Only if this build still owns the keys. A concurrent rebuild
          // installs its own `origin`, and its driver must not be evicted
          // because ours failed.
          if (driverOrigin[factoryKey] === origin) {
            invalidate();
          }

          if (driver) {
            await driver.release();
          }

          throw e;
        }
      })();

      // No separate pre-agg driver needed — share the same promise across keys
      aliasedKeys.forEach((key) => {
        driverPromise[key] = pending;
      });

      return pending;
    };

    const orchestratorApi = this.createOrchestratorApi(
      resolveDataSourceDriver,
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

    this.orchestratorRequestContexts.set(orchestratorApi, requestContextRef);
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
   * Resolve driver by the data source.
   */
  public async resolveDriver(
    context: DriverContext,
    options?: OrchestratorInitedOptions,
  ): Promise<BaseDriver> {
    return this.createDriverFromFactoryResult(
      await this.options.driverFactory(context),
      context,
      options,
    );
  }

  /**
   * Build a driver from whatever `driverFactory` returned. Split out of
   * `resolveDriver` so a caller that has already invoked the factory — to
   * compare its result against the cached driver's — can build from that same
   * result instead of invoking a user-supplied function a second time.
   */
  protected async createDriverFromFactoryResult(
    val: DriverConfig | BaseDriver,
    context: DriverContext,
    options?: OrchestratorInitedOptions,
  ): Promise<BaseDriver> {
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
      return CubejsServerCore.createDriver(type, opts);
    }
  }

  /**
   * Decide whether a cached driver still reflects what `driverFactory` would
   * resolve for the current request context.
   *
   * The check is deliberately layered so that deployments which cannot be
   * affected never leave the fast path, and no user-supplied function is called
   * more often than it has to be:
   *
   *  1. No custom `driverFactory`, or one that hands back a constructed driver
   *     rather than a config — nothing context-derived to compare. Reuse.
   *  2. The security context is byte-for-byte what the cached driver was built
   *     from. Reuse, without calling the factory at all. This is the common
   *     case: `requestId` changes per request, credentials do not.
   *  3. The security context changed, so ask the factory. Most factories ignore
   *     it and return an identical config — reuse, and remember the new context
   *     so step 2 short-circuits next time.
   *  4. The config genuinely changed. Rebuild.
   *
   * Step 4 is what fixes a rotated per-user credential: previously the driver
   * built from the first request's token was reused for the life of the
   * process, so every new connection it opened failed to authenticate.
   *
   * Note this follows the documented contract of `contextToOrchestratorId` —
   * that it is the cache key for database connections. Two contexts that
   * resolve to different connections but share an orchestrator id are a
   * misconfiguration; they were already sharing one user's connection before
   * this change.
   */
  protected async resolveDriverStaleness(
    origin: DriverOrigin | undefined,
    context: DriverContext,
  ): Promise<{ stale: false } | { stale: true, factoryResult: DriverFactoryResult }> {
    if (
      !origin ||
      origin.configFingerprint === null ||
      !this.optsHandler.isCustomDriverFactory()
    ) {
      return { stale: false };
    }

    const securityContextFingerprint = fingerprint(context.securityContext);

    if (
      securityContextFingerprint === null ||
      securityContextFingerprint === origin.securityContextFingerprint
    ) {
      return { stale: false };
    }

    let value: DriverConfig | BaseDriver;

    try {
      value = await this.options.driverFactory(context);
    } catch (error) {
      // This call is a probe, not the request's own resolution: a cache hit
      // never used to invoke the factory at all, so letting a transient failure
      // here propagate would fail a query the cached driver could have served.
      // Degrade to reuse, as with anything else that cannot be compared.
      this.logger('Driver staleness check error', {
        dataSource: context.dataSource,
        error: (error as Error).stack || (error as Error).toString(),
      });

      return { stale: false };
    }

    const configFingerprint = isDriver(value) ? null : fingerprint(value);

    if (configFingerprint === null || configFingerprint === origin.configFingerprint) {
      // A driver the factory constructed for this probe is about to be dropped,
      // so hand back whatever it opened rather than leaking it. Only reachable
      // for a factory that returns a config sometimes and a driver other times.
      if (isDriver(value)) {
        try {
          await (<BaseDriver>value).release();
        } catch (error) {
          this.logger('Driver release error', {
            dataSource: context.dataSource,
            error: (error as Error).stack || (error as Error).toString(),
          });
        }
      }

      origin.securityContextFingerprint = securityContextFingerprint;

      return { stale: false };
    }

    return { stale: true, factoryResult: { value, securityContextFingerprint } };
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
