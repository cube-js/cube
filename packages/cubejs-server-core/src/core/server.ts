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
  INVALID_REQUEST_ID_MESSAGE,
  isValidRequestId,
  pinPreAggregationsSchema,
  releasePreAggregationsSchemaPin,
  track,
  FileRepository,
  SchemaFileRepository,
  withLogRedaction,
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
import { parseDriverExpiry, withoutDriverExpiry } from './driver-config-expiry';
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
 * Minimum gap between rebuilds of one alias set, so a factory whose config is
 * not stable across calls (a per-call credential, a nonce) cannot churn the
 * pool. Inside the window the cached driver is reused without asking the factory.
 */
const DRIVER_REBUILD_MIN_INTERVAL_MS = 30 * 1000;

/**
 * Separate refusal incidents, spanning the grace window, before a driver the
 * factory keeps refusing to configure is given up rather than reused. One of two
 * routes to a give-up; the other is an incident outlasting `PROBE_FAILURE_GRACE_MS`.
 */
const MAX_PROBE_FAILURE_INCIDENTS = 3;

/**
 * How long refusal must last, as repeated incidents or one unbroken incident,
 * before the driver is given up. Minutes, so a dependency restarting inside the
 * factory does not drain a pool whose credential is still valid; an outage that
 * outlasts it does, which is why the docs tell factories to catch their own failures.
 */
const PROBE_FAILURE_GRACE_MS = 5 * 60 * 1000;

/**
 * How long a refusal stays on the record. Longer than the grace window so a
 * sparse deployment, which probes only when the security context changes, can
 * still reach the bound.
 */
const PROBE_FAILURE_RETENTION_MS = 30 * 60 * 1000;

/**
 * Refusals this close together are one incident, so a burst of concurrent probes
 * failing on one blink counts once.
 */
const PROBE_FAILURE_COALESCE_MS = 2 * 1000;

/**
 * What a cached driver was built from. `null` on either fingerprint means
 * "cannot tell whether it changed", which is always read as "assume it did
 * not"; `expiresAt` is undefined when the configuration named no lifetime.
 */
type DriverOrigin = {
  securityContextFingerprint: string | null;
  configFingerprint: string | null;
  expiresAt: number | undefined;
  /** Whether an unusable lifetime was reported, so the per-probe carry-over warns once. */
  lifetimeIgnoredReported: boolean;
};

/**
 * Refusal incidents for one alias set in a rolling window, re-based once
 * `lastFailureAt` is older than `PROBE_FAILURE_RETENTION_MS`.
 */
type DriverProbeFailures = {
  count: number;
  firstFailureAt: number;
  lastFailureAt: number;
  /** Start of the current unbroken incident, so one lasting the grace window still gives up. */
  incidentStartedAt: number;
};

/** A `driverFactory` result together with the context that produced it. */
type DriverFactoryResult = {
  value: DriverConfig | BaseDriver;
  securityContextFingerprint: string | null;
};

/** Why a cached driver was found stale, for the operator reading the log. */
type DriverStalenessReason = 'configuration change' | 'lifetime elapsed';

/** Every reason a cached driver is replaced; all are counted and rate-limited together. */
type DriverReplacementReason =
  | DriverStalenessReason
  | 'repeated staleness check failures';

/**
 * The verdict on a cached driver. `factoryResult` carries a probe's result so a
 * rebuild does not call the factory twice; `probeFailed` and `probeResolved` mark
 * a reuse where the factory threw or answered.
 */
type DriverStaleness =
  | { stale: false, probeFailed?: boolean, probeResolved?: boolean }
  | { stale: true, reason: DriverStalenessReason, factoryResult?: DriverFactoryResult };

/** Rebuild history of the one driver an alias set resolves to. */
type DriverRebuildState = {
  count: number;
  lastRebuildAt: number;
  /**
   * Whether the suppression window opened by that rebuild has already been
   * logged. Reset by each rebuild, so a thrashing deployment reports once per
   * window rather than once per query.
   */
  suppressionReported: boolean;
};

/**
 * Fingerprint of a driver configuration minus its lifetime: a factory recomputes
 * `expiresAt` on every call, and including it would rebuild the pool on a timer.
 */
function driverConfigFingerprint(value: DriverConfig): string | null {
  return fingerprint(withoutDriverExpiry(value));
}

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
   * Latest request context per cached orchestrator, read by its driver factory so a
   * context-derived driver can be rebuilt. Weak, so an entry dies with its api.
   */
  protected readonly orchestratorRequestContexts =
    new WeakMap<OrchestratorApi, { current: RequestContext }>();

  /**
   * In-flight orchestrator api builds, by id. Concurrent callers of a cold id must
   * share one build: `OrchestratorStorage` releases a replaced entry, so a second
   * build closes the Cube Store connection of the api the first caller is using.
   */
  protected readonly buildingOrchestratorApis: Map<string, Promise<OrchestratorApi>> = new Map();

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  protected repositoryFactory: ((context: RequestContext) => SchemaFileRepository) | (() => FileRepository);

  protected contextToDbType: DbTypeInternalFn;

  protected contextToExternalDbType: ExternalDbTypeFn;

  protected compilerCache: LRUCache<string, CompilerApi>;

  protected readonly contextToOrchestratorId: ContextToOrchestratorIdFn;

  protected readonly contextToCubeStoreRouterId: ContextToCubeStoreRouterIdFn | null;

  protected readonly preAggregationsSchema: PreAggregationsSchemaFn;

  /**
   * This instance's share of the process-wide pre-aggregation schema pin, when it took
   * one. Undefined when it did not, or once shutdown has released it.
   */
  private heldPreAggregationsSchemaPin: symbol | undefined;

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

    // Same resolution the gateway and OptsHandler do, so a `devServer: true` embedder
    // gets the dev logger and unredacted SQL, and a `devServer: false` one gets neither
    // from the env var alone
    const devMode = opts.devServer ?? getEnv('devMode');

    const logger = opts.logger || createLogger(
      !devMode,
      getEnv('logLevel'),
    );
    // Wraps the log sink only: the agent and telemetry wrappers installed below
    // sit outside it and forward the original params
    this.logger = getEnv('logRedaction', devMode) ? withLogRedaction(logger) : logger;

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
            error: params.redactedError ?? params.error,
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
            error: params.redactedError ?? params.error,
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

    // Last in the constructor, so anything that throws above takes no pin; shutdown
    // releases it
    if (typeof this.options.preAggregationsSchema === 'string') {
      this.heldPreAggregationsSchemaPin =
        pinPreAggregationsSchema(this.options.preAggregationsSchema);
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
          .send('<html><body>Cube server is running in production mode. <a href="https://docs.cube.dev/cube-core/deployment#production-checklist">Learn more about production mode</a>.</body></html>');
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
        devServer: this.options.devServer,
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
    // A build still in flight would otherwise keep handing its pre-reset api --
    // built from the pre-reset context and the env this is about to reload -- to
    // every caller arriving until it settles.
    this.buildingOrchestratorApis.clear();
    this.compilerCache.clear();

    this.reloadEnvVariables();

    this.repository = new FileRepository(this.options.schemaPath);
    this.repositoryFactory = this.options.repositoryFactory || (() => this.repository);

    this.startScheduledRefreshTimer();
  }

  public async getOrchestratorApi(context: RequestContext): Promise<OrchestratorApi> {
    const orchestratorId = await this.contextToOrchestratorId(context);

    if (this.orchestratorStorage.has(orchestratorId)) {
      return this.trackRequestContext(this.orchestratorStorage.get(orchestratorId), context);
    }

    const building = this.buildingOrchestratorApis.get(orchestratorId);

    if (building) {
      return building.then((orchestratorApi) => this.trackRequestContext(orchestratorApi, context));
    }

    // Registered before the first `await` in the build, so nothing can interleave
    // between the miss above and this line.
    const pending = this.buildOrchestratorApi(orchestratorId, context)
      .finally(() => {
        // Dropped once settled: a success is in the cache by now, and a failure must
        // not become the cached answer for this id. Identity-checked because
        // `resetInstanceState()` clears the map mid-build, after which the entry
        // belongs to a later caller's build rather than to this one.
        if (this.buildingOrchestratorApis.get(orchestratorId) === pending) {
          this.buildingOrchestratorApis.delete(orchestratorId);
        }
      });

    this.buildingOrchestratorApis.set(orchestratorId, pending);

    return pending;
  }

  /** Record `context` as the latest one `orchestratorApi` served, for every caller including joiners. */
  protected trackRequestContext(orchestratorApi: OrchestratorApi, context: RequestContext): OrchestratorApi {
    const contextRef = this.orchestratorRequestContexts.get(orchestratorApi);

    if (contextRef) {
      contextRef.current = context;
    }

    return orchestratorApi;
  }

  protected async buildOrchestratorApi(orchestratorId: string, context: RequestContext): Promise<OrchestratorApi> {
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

    /** Rebuild history per alias set, which rate-limits rebuilds and flags thrashing. */
    const driverRebuilds: Record<string, DriverRebuildState> = {};

    /** Refusal incidents per alias set; cleared only by a probe or build that reached the factory. */
    const driverProbeFailures: Record<string, DriverProbeFailures> = {};

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
       * pre-aggregation credentials both keys share one driver, so they are
       * written and invalidated together.
       */
      const aliasedKeys = hasSeparatePreAggEnv
        ? [factoryKey]
        : [dataSource, `${dataSource}@pre_agg`];

      const invalidate = () => aliasedKeys.forEach((key) => {
        driverPromise[key] = null;
        delete driverOrigin[key];
      });

      /**
       * Drop every key pointing at `driver`, so no alias hands out a draining
       * pool, and release it without awaiting: running queries finish, and a
       * release failure must not fail this request.
       */
      const replaceCachedDriver = (driver: Promise<BaseDriver>) => {
        Object.keys(driverPromise)
          .filter((key) => driverPromise[key] === driver)
          .forEach((key) => {
            driverPromise[key] = null;
            delete driverOrigin[key];
          });

        driver
          .then((resolved) => resolved.release())
          .catch((error) => this.logger('Driver release error', {
            dataSource,
            error: (error as Error).stack || (error as Error).toString(),
          }));
      };

      /** Per alias set, not per key, so a shared driver is one counter and one rebuild. */
      const rebuildKey = aliasedKeys[0];

      /**
       * Count, rate-limit and report a replacement. Every path that tears a pool
       * down goes through here, so none bypasses the interval or the diagnostic.
       */
      const recordDriverRebuild = (reason: DriverReplacementReason, warning: string) => {
        // Re-read rather than captured before the probe's await, so an increment is never lost.
        const state = driverRebuilds[rebuildKey]
          || { count: 0, lastRebuildAt: 0, suppressionReported: false };

        state.count += 1;
        state.lastRebuildAt = Date.now();
        state.suppressionReported = false;
        driverRebuilds[rebuildKey] = state;

        // Carries `warning` so it survives the default log level, which drops
        // plain-params messages.
        this.logger('Rebuilding driver', {
          dataSource,
          preAggregations,
          rebuildCount: state.count,
          reason,
          warning,
        });

        // A rotation rebuilds a few times a day; this many means contexts keep
        // displacing each other's driver, or the factory is unreliable.
        if (state.count === DRIVER_REBUILD_WARN_THRESHOLD) {
          this.logger('Driver rebuilt repeatedly', {
            dataSource,
            rebuildCount: state.count,
            warning: 'Driver keeps being replaced for one orchestrator. '
              + 'contextToOrchestratorId likely does not distinguish the contexts '
              + 'driverFactory returns different connections for, or driverFactory '
              + 'is not resolving a configuration reliably.',
          });
        }
      };

      // Already resolved by the staleness check below, so the factory is not
      // asked twice for the same rebuild.
      let resolvedFactoryResult: DriverFactoryResult | undefined;

      const cached = driverPromise[factoryKey];
      const rebuildState = driverRebuilds[rebuildKey];

      if (
        cached &&
        rebuildState &&
        Date.now() - rebuildState.lastRebuildAt < DRIVER_REBUILD_MIN_INTERVAL_MS
      ) {
        // Inside the window this is a plain cache hit, without asking the factory;
        // a real change is picked up once the window closes.
        if (!rebuildState.suppressionReported) {
          rebuildState.suppressionReported = true;

          // Carries `warning` so it survives the default log level, as the
          // rebuild it follows does.
          this.logger('Driver rebuild suppressed', {
            dataSource,
            preAggregations,
            rebuildCount: rebuildState.count,
            warning: 'Driver was rebuilt less than '
              + `${DRIVER_REBUILD_MIN_INTERVAL_MS / 1000}s ago; reusing it without `
              + 'rechecking its configuration. Sustained suppression means the '
              + 'configuration is not stable across driverFactory calls, or that '
              + 'contextToOrchestratorId does not distinguish the contexts '
              + 'driverFactory returns different connections for.',
          });
        }

        return cached;
      }

      if (cached) {
        const staleness = await this.resolveDriverStaleness(
          driverOrigin[factoryKey],
          driverContext(),
        );

        // `resolveDriverStaleness` awaits the user's factory, so another caller
        // may have replaced or invalidated this key in the meantime. Its work
        // supersedes ours, and `cached` is no longer ours to reuse or release:
        // it has either been handed to that caller's requests or already
        // released by it.
        const superseding = driverPromise[factoryKey];

        if (superseding !== cached) {
          // Retry so this request gets a driver matching its context, but bounded
          // so contexts displacing each other cannot starve it; past the bound,
          // take what is cached.
          if (attempt < MAX_DRIVER_REBUILD_ATTEMPTS) {
            return resolveDataSourceDriver(dataSource, preAggregations, attempt + 1);
          }

          if (superseding) {
            return superseding;
          }

          // Invalidated rather than replaced — the winning caller's own build
          // failed, so it released `cached` and left nothing to reuse. Build
          // below, which cannot recurse again, carrying the probe's result when
          // it already resolved one so the factory is not asked twice.
          resolvedFactoryResult = staleness.stale ? staleness.factoryResult : undefined;
        // `=== false` rather than `!`: this package compiles with
        // `strictNullChecks` off, where the negation does not narrow the union
        // and `probeFailed` below would not typecheck.
        } else if (staleness.stale === false) {
          if (!staleness.probeFailed) {
            // Only a probe that reached the factory clears refusals: clearing on a
            // plain fingerprint cache hit would let one context erase another's.
            if (staleness.probeResolved) {
              delete driverProbeFailures[rebuildKey];
            }

            return cached;
          }

          const now = Date.now();
          const previousFailures = driverProbeFailures[rebuildKey];

          // A rolling window, so flakes weeks apart do not add up to one outage.
          const failures = previousFailures
            && now - previousFailures.lastFailureAt < PROBE_FAILURE_RETENTION_MS
            ? previousFailures
            : {
              count: 0, firstFailureAt: now, lastFailureAt: now, incidentStartedAt: now,
            };

          // Refusals close to the last one seen are one incident; a continuous
          // stream is still bounded by its own duration below.
          if (
            failures.count === 0 ||
            now - failures.lastFailureAt >= PROBE_FAILURE_COALESCE_MS
          ) {
            failures.count += 1;
            failures.incidentStartedAt = now;
          }

          failures.lastFailureAt = now;
          driverProbeFailures[rebuildKey] = failures;

          const failingForMs = now - failures.firstFailureAt;

          // Repeated incidents catch sparse probing; one unbroken incident catches
          // busy traffic, where refusals coalesce into a single count.
          const sustainedIncident = now - failures.incidentStartedAt >= PROBE_FAILURE_GRACE_MS;
          const repeatedIncidents = failures.count >= MAX_PROBE_FAILURE_INCIDENTS
            && failingForMs >= PROBE_FAILURE_GRACE_MS;

          // Transient, as far as anything here can tell. Reuse.
          if (!sustainedIncident && !repeatedIncidents) {
            return cached;
          }

          recordDriverRebuild(
            'repeated staleness check failures',
            `driverFactory has failed every staleness check for ${
              Math.round(failingForMs / 1000)
            }s. Releasing the connection it built rather than serving queries on `
            + 'a configuration it will no longer produce; the next request calls '
            + 'the factory itself, so a factory that fails closed on an unusable '
            + 'credential surfaces its own error.',
          );

          delete driverProbeFailures[rebuildKey];
          replaceCachedDriver(cached);

          // Falls through to the build below, which calls the factory itself:
          // it either recovers, or throws where the caller can see it.
        } else {
          // Opens a fresh suppression window, so the next configuration change
          // for this alias set waits it out rather than tearing down the pool
          // this rebuild is about to stand up.
          recordDriverRebuild(
            staleness.reason,
            `Replacing the connection — ${staleness.reason}.`,
          );

          delete driverProbeFailures[rebuildKey];
          replaceCachedDriver(cached);

          resolvedFactoryResult = staleness.factoryResult;
        }
      }

      if (preAggregations && hasSeparatePreAggEnv && this.optsHandler.isCustomDriverFactory()) {
        this.logger('Pre-aggregation driver conflict', {
          error: 'Both driverFactory and PRE_AGGREGATIONS env vars are defined. driverFactory will take precedence.',
          dataSource,
        });
      }

      // Shared by reference across `aliasedKeys`. Empty until the factory is
      // called, which `resolveDriverStaleness` reads as reuse.
      const origin: DriverOrigin = {
        securityContextFingerprint: null,
        configFingerprint: null,
        expiresAt: undefined,
        lifetimeIgnoredReported: false,
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

          const factoryConfig = isDriver(factoryResult.value)
            ? undefined
            : <DriverConfig>factoryResult.value;

          origin.securityContextFingerprint = factoryResult.securityContextFingerprint;
          origin.configFingerprint = factoryConfig
            ? driverConfigFingerprint(factoryConfig)
            : null;
          origin.expiresAt = factoryConfig
            ? this.resolveBuiltDriverExpiry(factoryConfig, dataSource, origin)
            : undefined;

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

            // Resolved a configuration and stood a connection up on it, so
            // whatever the probes were failing on has passed.
            delete driverProbeFailures[rebuildKey];

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
        // Deliberately resolved from the creating `context`, outside the staleness
        // check: the external store and the data source's db type are not per-user.
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

    // Comes from server config, so only warn: rejecting it would stop the tenant's refresh
    if (result?.requestId && !isValidRequestId(result.requestId)) {
      this.logger('Refresh Scheduler Warning', {
        warning: `Invalid requestId ${JSON.stringify(result.requestId)} in scheduled refresh context. ${INVALID_REQUEST_ID_MESSAGE}`,
      });
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
      // Without the lifetime: it describes when to replace this driver, not
      // how to connect, and every other key here is passed to the driver's own
      // constructor.
      const { type, ...rest } = withoutDriverExpiry(<DriverConfig>val);
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
   * The lifetime to hold a driver to. A newly stated deadline that has passed, or
   * is shorter than `DRIVER_REBUILD_MIN_INTERVAL_MS`, is ignored with a warning:
   * honouring it would rebuild the pool once per window for the life of the process.
   */
  protected resolveBuiltDriverExpiry(
    config: DriverConfig,
    dataSource: string,
    origin: DriverOrigin,
  ): number | undefined {
    const expiresAt = parseDriverExpiry(config.expiresAt);

    if (expiresAt === undefined) {
      return undefined;
    }

    // Already judged when installed. Re-measuring it on carry-over would drop a
    // good deadline in its final window, exactly when the lifetime matters.
    if (expiresAt === origin.expiresAt) {
      return expiresAt;
    }

    const remainingMs = expiresAt - Date.now();

    if (remainingMs >= DRIVER_REBUILD_MIN_INTERVAL_MS) {
      return expiresAt;
    }

    // Once per driver: the carry-over path runs on every security context change.
    if (!origin.lifetimeIgnoredReported) {
      origin.lifetimeIgnoredReported = true;

      this.logger('Driver lifetime ignored', {
        dataSource,
        expiresAt: new Date(expiresAt).toISOString(),
        warning: remainingMs <= 0
          ? 'driverFactory returned a configuration whose expiresAt has already '
            + 'passed. Using the connection anyway and ignoring the lifetime: '
            + 'replacing a driver cannot move a deadline the factory keeps '
            + 're-asserting, and honouring it would rebuild the pool for the life '
            + 'of the process. expiresAt must state when the credential being '
            + 'returned stops being usable, in the future.'
          : 'driverFactory returned a configuration whose expiresAt is less than '
            + `${DRIVER_REBUILD_MIN_INTERVAL_MS / 1000}s away, which is shorter `
            + 'than the interval replacements are rate-limited to. Honouring it '
            + 'would replace the connection once per window for the life of the '
            + 'process, so the lifetime is ignored; a credential rotating that '
            + 'fast is picked up by its configuration changing instead.',
      });
    }

    // Keep a deadline already accepted. It is still in the future here, because
    // an elapsed one is found stale before the factory is asked.
    return origin.expiresAt;
  }

  /**
   * Decide whether a cached driver still reflects what `driverFactory` would
   * resolve for the current request context. A `null` fingerprint anywhere means
   * "cannot tell" and is read as reuse, never as stale.
   */
  protected async resolveDriverStaleness(
    origin: DriverOrigin | undefined,
    context: DriverContext,
  ): Promise<DriverStaleness> {
    if (!origin) {
      return { stale: false };
    }

    // Checked first, without asking the factory: a credential that stopped
    // rotating resolves to the same configuration while its connection is dead.
    if (origin.expiresAt !== undefined && Date.now() >= origin.expiresAt) {
      return { stale: true, reason: 'lifetime elapsed' };
    }

    if (
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
      // A probe, not the request's own resolution: reuse rather than fail a query
      // the cached driver could serve, and report it so sustained refusal is bounded.
      this.logger('Driver staleness check error', {
        dataSource: context.dataSource,
        error: (error as Error).stack || (error as Error).toString(),
      });

      return { stale: false, probeFailed: true };
    }

    // A constructed driver has no config to compare: treat it as unchanged, and never
    // release it, because it belongs to the factory.
    const config = isDriver(value) ? undefined : <DriverConfig>value;
    const configFingerprint = config ? driverConfigFingerprint(config) : null;

    if (configFingerprint === null || configFingerprint === origin.configFingerprint) {
      origin.securityContextFingerprint = securityContextFingerprint;

      // `expiresAt` is outside the fingerprint, so carry a re-issued deadline
      // over, through the build path's guard so an elapsed one is not reinstated.
      if (config) {
        origin.expiresAt = this.resolveBuiltDriverExpiry(config, context.dataSource, origin);
      }

      // The only reuse that reached the factory, so the only one that is
      // evidence about whether it is still refusing.
      return { stale: false, probeResolved: true };
    }

    return {
      stale: true,
      reason: 'configuration change',
      factoryResult: { value, securityContextFingerprint },
    };
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
      console.log('🛑 Cube Server requires locally running Redis instance to connect to');
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

    // Undefined when this instance never took a share, and cleared here because this
    // method is public and unguarded: a second call must not release the share again
    if (this.heldPreAggregationsSchemaPin !== undefined) {
      const holder = this.heldPreAggregationsSchemaPin;

      this.heldPreAggregationsSchemaPin = undefined;
      releasePreAggregationsSchemaPin(holder);
    }

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
