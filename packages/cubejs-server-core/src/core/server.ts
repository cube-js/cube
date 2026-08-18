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
 * How long a data source keeps a freshly rebuilt driver before another
 * configuration change may replace it. Inside the window the cached driver is
 * reused without even asking the factory, exactly as before this file learned
 * to rebuild at all.
 *
 * A rebuild tears down a connection pool, so the rate has to be bounded by
 * something other than how often contexts happen to differ. Without this, a
 * deployment whose `driverFactory` returns a configuration that is not stable
 * across calls — a credential minted per call, say, or one carrying a nonce —
 * would rebuild on request after request. That deployment works today, because
 * the driver is resolved once and the difference is never noticed; it must not
 * be turned into pool churn.
 *
 * The cost is that a credential which rotates twice inside one window is picked
 * up a window late rather than immediately. At half a minute against
 * credentials that live for an hour, that is not a tradeoff worth agonising
 * over — and the alternative, before this change, was "not until redeploy".
 */
const DRIVER_REBUILD_MIN_INTERVAL_MS = 30 * 1000;

/**
 * Separate refusal incidents, spanning the grace window, before the cached
 * driver is given up rather than reused. One of the two routes to a give-up —
 * see `PROBE_FAILURE_GRACE_MS` for the other, and `PROBE_FAILURE_COALESCE_MS`
 * for why the unit is incidents rather than failed checks.
 *
 * A probe failure is the factory declining to produce a connection for this
 * context. One is transient — a secret store blinking, a timeout — and reusing
 * what is cached is right. Sustained refusal is not: a `driverFactory` written
 * to fail closed on an unusable credential is stating that this connection must
 * not serve queries, and honouring that only when the factory happens to return
 * is how an expired credential goes on serving errors from a pool nobody
 * rebuilds.
 */
const MAX_PROBE_FAILURE_INCIDENTS = 3;

/**
 * How long refusal has to go on before the driver is given up — both how long
 * repeated incidents must span, and how long a single unbroken one must run.
 *
 * Those are the two routes, and each covers a traffic profile the other cannot
 * reach. Where probes are sparse every refusal stands alone, so the count is
 * what accumulates. Where they are dense they coalesce into one incident that
 * never ends, and only its duration distinguishes it from a blink.
 *
 * Note what this means, because it is the thing to check before assuming
 * otherwise: one continuous dependency outage *is* enough, if it lasts. That is
 * deliberate — it is the same call this bound made when the window was widened
 * to minutes, and the recipe tells a `driver_factory` reaching an external
 * dependency to catch its own failures rather than propagate them.
 *
 * Minutes rather than seconds because a probe failure is not evidence about the
 * cached connection — it is evidence about whatever the factory had to reach to
 * answer. A secret store restarting, a token endpoint returning 503, a DNS blip
 * inside the factory: in every one of those the cached credential is untouched
 * and still valid, and giving the pool up makes a dependency's outage into a
 * query outage. A credential that has genuinely stopped working is not urgent
 * to the second, so the bar is set where a dependency can restart under it.
 */
const PROBE_FAILURE_GRACE_MS = 5 * 60 * 1000;

/**
 * How long one refusal stays on the record before it is forgotten.
 *
 * Deliberately separate from the grace window, because the two pull opposite
 * ways. The grace window wants to be long, so a dependency can restart under
 * it. Retention wants to be long enough that a *sparse* deployment can still
 * reach the bound: probes are only issued when the security context fingerprint
 * changes, so a few-user deployment may probe once every several minutes, and
 * if a refusal expired at the grace window such a deployment would reset to one
 * every time and never give up a credential however permanently dead it was.
 *
 * Longer than the grace window, then, but far short of the days-apart flakes
 * that made a never-expiring record wrong: a refusal half an hour stale is not
 * evidence about the one happening now.
 *
 * Retention this long would, on its own, let two unrelated blinks half an hour
 * apart reach the bound between them. What keeps that from happening is that
 * the count is of incidents rather than of refusals — see
 * `PROBE_FAILURE_COALESCE_MS`.
 */
const PROBE_FAILURE_RETENTION_MS = 30 * 60 * 1000;

/**
 * How close together two refusals have to be to count as one.
 *
 * Retention outliving the grace window is what makes the bound reachable in a
 * sparse deployment, but on its own it also makes it reachable across unrelated
 * incidents: concurrent probes all fail on one blink of a dependency, and a
 * burst of three plus a single refusal six minutes later would otherwise
 * satisfy both conditions and drain a working pool for what was two brief
 * outages.
 *
 * Counting incidents rather than refusals removes that without giving the
 * sparse case back: a burst is one, and the bound still wants three. An
 * incident is bounded by its own duration as well as by the count, so a
 * refusal stream arriving faster than this window is caught by having lasted
 * rather than by being counted.
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
  /**
   * Whether this driver's unusable lifetime has been reported. The carry-over
   * that resolves it runs on every security context change, with no rate limit
   * of its own, so without this the warning is emitted per request — trading
   * the pool churn this guard removes for log volume on the hot path.
   */
  lifetimeIgnoredReported: boolean;
};

/**
 * Probe failures for one alias set inside one rolling window: how many, when
 * the window opened, and when it was last extended.
 *
 * `lastFailureAt` is what makes the window rolling, against
 * `PROBE_FAILURE_RETENTION_MS`. Probes are only issued when the security
 * context fingerprint changes, so in a quiet deployment two of them can be
 * hours apart with nothing in between to clear the count — and three unrelated
 * flakes on three different days are not a sustained refusal, however they look
 * to a counter that only ever goes up.
 */
type DriverProbeFailures = {
  count: number;
  firstFailureAt: number;
  lastFailureAt: number;
  /**
   * When the incident being counted began — the first refusal of the current
   * unbroken run, rather than of the window. An incident that has itself lasted
   * the grace window is no longer a blink, which is what keeps a continuous
   * stream of refusals from coalescing into one uncountable incident forever.
   */
  incidentStartedAt: number;
};

/** A `driverFactory` result together with the context that produced it. */
type DriverFactoryResult = {
  value: DriverConfig | BaseDriver;
  securityContextFingerprint: string | null;
};

/** Why a cached driver was found stale, for the operator reading the log. */
type DriverStalenessReason = 'configuration change' | 'lifetime elapsed';

/**
 * Every reason a cached driver is replaced. A refusal is not a staleness
 * verdict — the factory never produced a configuration to compare — but it
 * tears down the same connection pool, so it is counted and rate-limited
 * alongside the verdicts rather than slipping past both brakes.
 */
type DriverReplacementReason =
  | DriverStalenessReason
  | 'repeated staleness check failures';

/**
 * The verdict on a cached driver. `factoryResult` is present only when the
 * probe already resolved one, so the rebuild does not call the factory twice;
 * `probeFailed` marks the reuse that happened because the factory threw, which
 * the caller counts.
 */
type DriverStaleness =
  | { stale: false, probeFailed?: boolean }
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
 * Fingerprint of everything in a driver configuration that identifies the
 * connection — which is all of it except the lifetime.
 *
 * The lifetime is excluded deliberately. It is enforced on its own, and it is
 * the one field a factory is expected to return a different value for on every
 * call, being a deadline recomputed from whatever credential it just read.
 * Including it would read each of those calls as a changed connection and
 * rebuild the pool on a timer.
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
     * Rebuild history per alias set, which both rate-limits rebuilds and makes
     * a deployment whose `contextToOrchestratorId` does not partition by
     * whatever `driverFactory` reads — every user sharing one orchestrator, say
     * — diagnosable: it keeps resolving a changed configuration rather than
     * doing so once per credential rotation.
     */
    const driverRebuilds: Record<string, DriverRebuildState> = {};

    /**
     * Consecutive staleness probes that threw, per alias set. Reset by any
     * probe or build that resolves a configuration, so only *sustained* refusal
     * reaches the bound.
     */
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

      /**
       * Drop every key pointing at `driver` and release it off the request path.
       *
       * Every key, not just the one asked for: a surviving alias would keep
       * handing out a driver whose pool is being drained, and would release it a
       * second time when it was itself found stale.
       *
       * `release` drains the pool, so queries already running on the replaced
       * driver finish before its connections close. It is deliberately not
       * awaited — this request should not wait on the previous driver's
       * in-flight work — and its failure must not fail this request.
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

      /**
       * Rebuilds are counted and rate-limited per alias set, not per key: a
       * rotation seen first through `default@pre_agg` and then through `default`
       * is one rebuild of one shared driver, and must not read as two counters
       * at 1 — nor rebuild twice.
       */
      const rebuildKey = aliasedKeys[0];

      /**
       * Count a replacement against the alias set's rebuild history, open a
       * suppression window on it, and report it.
       *
       * Both paths that tear a pool down come through here — a configuration
       * the factory changed, and a factory that will no longer produce one.
       * They cost the same thing, so they are bounded by the same state: a
       * replacement that skipped this would rebuild straight past the interval
       * that exists to stop pool churn, and never reach the diagnostic that
       * names it.
       */
      const recordDriverRebuild = (reason: DriverReplacementReason, warning: string) => {
        // Re-read rather than reusing what was captured before the staleness
        // probe awaited: reaching here means no concurrent rebuild landed, but
        // the count is the one piece of state that would silently lose an
        // increment if that ever stopped being true.
        const state = driverRebuilds[rebuildKey]
          || { count: 0, lastRebuildAt: 0, suppressionReported: false };

        state.count += 1;
        state.lastRebuildAt = Date.now();
        state.suppressionReported = false;
        driverRebuilds[rebuildKey] = state;

        // Carries `warning` so it survives the default log level: a plain-params
        // message matches no allowlist in `prodLogger`/`devLogger` and is
        // dropped below `trace`. Tearing down a connection pool is an event an
        // operator needs to be able to correlate against, and the threshold
        // message below arrives too late to reconstruct the first rebuilds.
        this.logger('Rebuilding driver', {
          dataSource,
          preAggregations,
          rebuildCount: state.count,
          reason,
          warning,
        });

        // A credential rotation rebuilds a handful of times a day. Rebuilding
        // this often means the orchestrator id does not partition by whatever
        // the factory reads, so contexts that need different connections keep
        // displacing each other's driver — or that the factory is not resolving
        // reliably enough to keep any connection.
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
        // Inside the window this is a plain cache hit: the factory is not asked
        // whether anything changed, because acting on the answer is what has to
        // be rate-limited and asking a user-supplied function on every query is
        // not free either. A configuration that really did change is picked up
        // by the first resolution after the window closes.
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
            delete driverProbeFailures[rebuildKey];

            return cached;
          }

          const now = Date.now();
          const previousFailures = driverProbeFailures[rebuildKey];

          // A rolling window, not a running total. Probes are only issued when
          // the context changes, so a record that is never re-based would add
          // up occasional flakes weeks apart and read them as one outage.
          // Retention rather than the grace window, so that a deployment
          // probing less often than the grace window can still reach the bound.
          const failures = previousFailures
            && now - previousFailures.lastFailureAt < PROBE_FAILURE_RETENTION_MS
            ? previousFailures
            : {
              count: 0, firstFailureAt: now, lastFailureAt: now, incidentStartedAt: now,
            };

          // Requests that arrived together and failed on the same blink of a
          // dependency are one refusal, not one each. The gap is measured
          // against the last refusal seen rather than the last one counted, so
          // that a continuous stream stays one incident — which is only sound
          // because an incident is also bounded by its own duration below.
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

          // Two shapes of sustained refusal, because either alone leaves a
          // traffic profile uncovered. Repeated incidents catch a deployment
          // whose probes are sparse enough that each refusal stands alone; one
          // unbroken incident catches a busy deployment, where refusals arrive
          // faster than the coalescing window and would otherwise count once
          // however long the credential stayed dead.
          const sustainedIncident = now - failures.incidentStartedAt >= PROBE_FAILURE_GRACE_MS;
          const repeatedIncidents = failures.count >= MAX_PROBE_FAILURE_INCIDENTS
            && failingForMs >= PROBE_FAILURE_GRACE_MS;

          // Transient, as far as anything here can tell. Reuse, exactly as
          // before this bound existed.
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

      // Shared by reference across `aliasedKeys`, so every key describes the
      // one driver they all resolve to. Starts empty: until the factory has
      // been called there is nothing to compare against, and
      // `resolveDriverStaleness` reads that as "reuse".
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
        // Deliberately outside the staleness check that `resolveDataSourceDriver`
        // applies to `requestContextRef.current`: this and `contextToDbType`
        // below keep resolving from `context`, the request that created the
        // orchestrator, and resolve once for its lifetime.
        //
        // Both were pinned that way before rebuilding existed, and neither is
        // the shape the rebuild is for. The external store is Cube Store or a
        // shared pre-aggregation warehouse — one connection the deployment owns,
        // not one derived from who is asking — and a data source's type does not
        // change per user, only its credentials do. Widening the rebuild to
        // cover them would mean tearing down the pre-aggregation store's pool on
        // a per-user signal that says nothing about it.
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
   * The lifetime to hold a driver to, given the configuration it was just built
   * from — and `undefined` where that configuration named a deadline that had
   * already passed.
   *
   * Rebuilding cannot fix a deadline the factory keeps re-asserting. Honouring
   * one would find the new driver stale the moment its suppression window
   * closed, tear down a pool it had just stood up, and resolve the same
   * unusable deadline again, for the life of the process. A driver built from
   * such a configuration is no worse than the one it replaced, so the
   * connection is kept and the lifetime dropped — the operator gets a warning
   * naming the field rather than churn that never resolves.
   *
   * Two deadlines are unusable, and they produce the same loop. One has already
   * passed. The other is shorter than the interval replacements are rate-limited
   * to: the driver is stale again the moment its suppression window closes, so
   * the rate limiter can never let this mechanism honour it. Dropping it strands
   * nothing — a credential that short is rotating, and rotation changes the
   * configuration, which is caught by comparison rather than by lifetime.
   *
   * The documented recipe does not reach either: its `accessToken()` withholds a
   * token that is already near expiry, so the configuration changes to the
   * service account, which names no lifetime, and converges. A factory passing
   * the provider's `accessTokenExpiresAt` straight through does reach them.
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

    // Already judged when it was installed. This also runs on every probe that
    // carries an unchanged configuration over, where what is left of the
    // deadline is a measure of time passing rather than of anything the factory
    // stated. Measuring it there would drop a perfectly good deadline once it
    // entered its final window — and leave the driver with no lifetime at all,
    // in precisely the stretch the lifetime exists to cover.
    if (expiresAt === origin.expiresAt) {
      return expiresAt;
    }

    const remainingMs = expiresAt - Date.now();

    if (remainingMs >= DRIVER_REBUILD_MIN_INTERVAL_MS) {
      return expiresAt;
    }

    // Once per driver, not once per call: the carry-over path resolves this on
    // every security context change, and the operator needs the field named
    // once, not on every query that arrives with a fresh JWT.
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

    // Keep whatever was accepted, if anything. The newly stated deadline cannot
    // be honoured, but one this driver is already held to can: an installed
    // deadline is still in the future here, because an elapsed one returns
    // `stale` from the lifetime check before the factory is ever asked. At the
    // build path this is `undefined`, so nothing changes there.
    return origin.expiresAt;
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
   * A `stale: true` verdict is permission to rebuild, not an instruction to: the
   * caller rate-limits rebuilds per data source, because the rate at which a
   * configuration appears to change is a property of user code, while the cost
   * of acting on it is a connection pool.
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
  ): Promise<DriverStaleness> {
    if (!origin) {
      return { stale: false };
    }

    // Checked first, and without asking the factory: a credential that has
    // stopped rotating resolves to the same configuration indefinitely while
    // the connection built from it is already dead. That is the one staleness a
    // comparison cannot see, which is why a configuration may state its own
    // lifetime.
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
      // This call is a probe, not the request's own resolution: a cache hit
      // never used to invoke the factory at all, so letting a transient failure
      // here propagate would fail a query the cached driver could have served.
      // Degrade to reuse, as with anything else that cannot be compared — but
      // report it, because a factory that keeps refusing is not transient and
      // the caller gives the driver up once these stop being occasional.
      this.logger('Driver staleness check error', {
        dataSource: context.dataSource,
        error: (error as Error).stack || (error as Error).toString(),
      });

      return { stale: false, probeFailed: true };
    }

    // `null` for a constructed driver, which carries no configuration to
    // compare — and, like every other `null` here, is read as "assume
    // unchanged". Nothing is released on that path: the value belongs to the
    // factory, which may be handing out a singleton it expects to keep working.
    //
    // No factory can actually reach it. One that returns drivers consistently
    // recorded a null config fingerprint on its first build and is rejected by
    // the guard above before the factory is ever called; one that switches from
    // configs to drivers is rejected by `OptsHandler.assertDriverFactoryResult`,
    // and that throw is caught above as a probe failure. It is handled because
    // the type admits it, not because it happens.
    const config = isDriver(value) ? undefined : <DriverConfig>value;
    const configFingerprint = config ? driverConfigFingerprint(config) : null;

    if (configFingerprint === null || configFingerprint === origin.configFingerprint) {
      origin.securityContextFingerprint = securityContextFingerprint;

      // The connection is unchanged, but its deadline may not be — the lifetime
      // is excluded from the fingerprint, so a credential re-issued with the
      // same value and a later expiry compares equal. Carrying the new deadline
      // over is what keeps that from rebuilding on the old one, once per window,
      // forever. Guarded like the build path, because a factory re-asserting an
      // elapsed deadline would otherwise reinstate it here on the next context
      // change, reopening the loop that guard exists to close.
      if (config) {
        origin.expiresAt = this.resolveBuiltDriverExpiry(config, context.dataSource, origin);
      }

      return { stale: false };
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
