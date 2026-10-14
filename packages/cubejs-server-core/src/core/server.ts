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

import type { Application as ExpressApplication, Response as ExpressResponse, NextFunction as ExpressNextFunction } from 'express';

import { BaseDriver, DriverFactoryByDataSource } from '@cubejs-backend/query-orchestrator';
import type { Request as ApiGatewayRequest, SubscriptionServer, WebSocketSendMessageFn } from '@cubejs-backend/api-gateway';

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
  DbTypeInternalContext,
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

/** A failed swap to one driver version is not retried for this long. */
const DRIVER_SWAP_FAILURE_BACKOFF_MS = 10 * 1000;

/** Swaps of one orchestrator closer together than this are reported as ping-pong. */
const DRIVER_SWAP_PING_PONG_MS = 5 * 1000;

/** `driverVersion` bookkeeping for one orchestrator api. */
type DriverVersionState = {
  orchestratorId: string;
  version: string | undefined;
  /** Context drivers are built from, moved to the swapping request's context on commit. */
  ctxRef: { current: RequestContext };
  swapping?: { version: string, promise: Promise<void> };
  lastFailure?: { version: string, at: number, reported: boolean };
  lastSwapAt?: number;
  swapDrivers: (context: RequestContext, version: string) => Promise<void>;
};

type SwappedDriverRelease = {
  timer?: NodeJS.Timeout;
  release: () => Promise<void>;
};

function driverSwapReleaseDelayFromEnv(): number {
  const value = process.env.CUBEJS_DRIVER_SWAP_RELEASE_DELAY;

  if (value === undefined || value === '') {
    return 600;
  }

  const seconds = Number(value);

  if (!Number.isInteger(seconds) || seconds < 0) {
    throw new Error(
      `Invalid CUBEJS_DRIVER_SWAP_RELEASE_DELAY "${value}": must be a non-negative integer number of seconds.`
    );
  }

  return seconds;
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
   * In-flight orchestrator api builds, by id. Concurrent callers of a cold id must
   * share one build: `OrchestratorStorage` releases a replaced entry, so a second
   * build closes the Cube Store connection of the api the first caller is using.
   */
  protected readonly buildingOrchestratorApis: Map<string, Promise<OrchestratorApi>> = new Map();

  protected readonly driverVersionStates: WeakMap<OrchestratorApi, DriverVersionState> = new WeakMap();

  /** Drivers a swap replaced and has not released yet; shutdown releases them at once. */
  protected readonly swappedDriverReleases: Set<SwappedDriverRelease> = new Set();

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  protected repositoryFactory: ((context: RequestContext) => SchemaFileRepository) | (() => FileRepository);

  protected contextToDbType: DbTypeInternalFn;

  protected contextToExternalDbType: ExternalDbTypeFn;

  protected compilerCache: LRUCache<string, CompilerApi>;

  protected readonly contextToOrchestratorId: ContextToOrchestratorIdFn;

  protected readonly contextToCubeStoreRouterId: ContextToCubeStoreRouterIdFn | null;

  /** Seconds; see `CreateOptions.driverSwapReleaseDelay`. */
  protected readonly driverSwapReleaseDelay: number;

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
    this.driverSwapReleaseDelay = this.options.driverSwapReleaseDelay ?? driverSwapReleaseDelayFromEnv();

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

  protected async contextRejectionMiddleware(req: ApiGatewayRequest, res: ExpressResponse, next: ExpressNextFunction) {
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
          dbType: async (dataSourceContext: DbTypeInternalContext) => {
            const dbType = await this.contextToDbType({ ...context, ...dataSourceContext });
            return dbType;
          },
          externalDbType: this.contextToExternalDbType(context),
          dialectClass: (dialectContext: { dataSource: string; dbType: string }) => (
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
          onCompiled: () => this.evictCompilersOverMemberBudget(appId),
        },
      );

      this.compilerCache.set(appId, compilerApi);
    }

    compilerApi.schemaVersion = currentSchemaVersion;
    return compilerApi;
  }

  /**
   * Never evicts `keepAppId`, nor the last model holding members, whatever its size:
   * a model alone over budget would otherwise recompile on every request.
   */
  protected evictCompilersOverMemberBudget(keepAppId: string): void {
    const budget = this.options.maxCompiledMembers;

    if (!budget) {
      return;
    }

    let total = 0;
    let models = 0;

    for (const api of this.compilerCache.values()) {
      total += api.compiledMemberCount;
      models += api.compiledMemberCount ? 1 : 0;
    }

    if (total <= budget) {
      return;
    }

    const evicted: string[] = [];

    // rkeys() walks least-recently-used first, so the models that go are the
    // ones longest unused. peek() rather than get(), which would mark the
    // model as freshly used and reorder the very list being walked.
    for (const appId of [...this.compilerCache.rkeys()]) {
      if (total <= budget) {
        break;
      }

      const api = appId === keepAppId ? undefined : this.compilerCache.peek(appId);

      // A model still compiling has no member count yet. Dropping one frees
      // nothing, and disposing a compile in flight only makes its tenant
      // start over -- so skip anything with nothing to free.
      if (api?.compiledMemberCount) {
        // The floor `keepAppId` gives fails when that app is no longer cached,
        // e.g. an evicted instance recompiling. Walking LRU first, this keeps
        // the most recently used model.
        if (models <= 1) {
          break;
        }

        total -= api.compiledMemberCount;
        models -= 1;
        this.compilerCache.delete(appId);
        evicted.push(appId);
      }
    }

    if (evicted.length) {
      this.logger('Compiler cache member budget exceeded', {
        budget,
        membersAfterEviction: total,
        evictedAppIds: evicted.length,
        compiledAppIds: this.compilerCache.size,
      });
    }
  }

  public async resetInstanceState() {
    await this.orchestratorStorage.releaseConnections();
    await this.releaseSwappedDrivers();

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
      return this.checkDriverVersion(this.orchestratorStorage.get(orchestratorId), context);
    }

    const building = this.buildingOrchestratorApis.get(orchestratorId);

    if (building) {
      return building.then((orchestratorApi) => this.checkDriverVersion(orchestratorApi, context));
    }

    // Registered before the first `await` in the build, so nothing can interleave
    // between the miss above and this line. The build records this context's
    // driver version, so the creator skips the check.
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

  /**
   * Swaps the drivers of `orchestratorApi` when `driverVersion` gives `context` a
   * version other than the one they were built for. Only the request that starts
   * a failing swap sees its error; others keep the old drivers.
   */
  protected async checkDriverVersion(orchestratorApi: OrchestratorApi, context: RequestContext): Promise<OrchestratorApi> {
    const state = this.driverVersionStates.get(orchestratorApi);

    if (!state || !this.options.driverVersion) {
      return orchestratorApi;
    }

    const requested = await this.resolveDriverVersion(context);

    if (requested === undefined) {
      return orchestratorApi;
    }

    while (requested !== state.version) {
      if (state.swapping) {
        await state.swapping.promise.catch(() => undefined);
      } else if (this.isDriverSwapBackingOff(state, requested)) {
        return orchestratorApi;
      } else {
        const promise = state.swapDrivers(context, requested)
          .then(
            () => {
              state.lastFailure = undefined;
            },
            (error) => {
              state.lastFailure = { version: requested, at: Date.now(), reported: false };

              this.logger('Driver Swap Error', {
                orchestratorId: state.orchestratorId,
                error: (error as Error).stack || String(error),
              });

              throw error;
            },
          )
          .finally(() => {
            state.swapping = undefined;
          });

        state.swapping = { version: requested, promise };

        await promise;
      }
    }

    return orchestratorApi;
  }

  /** Whether a swap to `requested` failed too recently to be retried. */
  protected isDriverSwapBackingOff(state: DriverVersionState, requested: string): boolean {
    const { lastFailure } = state;

    if (
      !lastFailure ||
      lastFailure.version !== requested ||
      Date.now() - lastFailure.at >= DRIVER_SWAP_FAILURE_BACKOFF_MS
    ) {
      return false;
    }

    if (!lastFailure.reported) {
      lastFailure.reported = true;

      this.logger('Driver Swap Backoff', {
        orchestratorId: state.orchestratorId,
        warning: `Swapping drivers to a new driverVersion failed less than ${DRIVER_SWAP_FAILURE_BACKOFF_MS / 1000}s ago. ` +
          'Serving with the previous drivers until it is retried.',
      });
    }

    return true;
  }

  /** Errors read as no version: the readiness probe asks with an empty context. */
  protected async resolveDriverVersion(context: RequestContext): Promise<string | undefined> {
    try {
      return (await this.options.driverVersion(context)) ?? undefined;
    } catch (error) {
      this.logger('Driver Version Error', {
        error: (error as Error).stack || String(error),
      });

      return undefined;
    }
  }

  /**
   * Releases a driver replaced by a swap once work that already holds it, such as
   * a stream or a pre-aggregation build, had time to finish: release destroys or
   * drains it at once.
   */
  protected scheduleSwappedDriverRelease(driver: BaseDriver) {
    const entry: SwappedDriverRelease = {
      release: async () => {
        clearTimeout(entry.timer);
        this.swappedDriverReleases.delete(entry);

        await this.releaseDriverSafely(driver);
      },
    };

    // Past 2^31-1 ms setTimeout fires at once, which would release the driver under its queries
    entry.timer = setTimeout(entry.release, Math.min(this.driverSwapReleaseDelay * 1000, 2 ** 31 - 1));
    entry.timer.unref();

    this.swappedDriverReleases.add(entry);
  }

  protected async releaseSwappedDrivers() {
    await Promise.all([...this.swappedDriverReleases].map((entry) => entry.release()));
  }

  protected async releaseDriverSafely(driver: BaseDriver) {
    try {
      if (driver.release) {
        await driver.release();
      }
    } catch (error) {
      this.logger('Driver Release Error', {
        error: (error as Error).stack || String(error),
      });
    }
  }

  protected async buildOrchestratorApi(orchestratorId: string, context: RequestContext): Promise<OrchestratorApi> {
    /**
     * Hash table to store promises which will be resolved with the
     * datasource drivers. DriverFactoryByDataSource function is closure
     * this constant.
     */
    const driverPromise: Record<string, Promise<BaseDriver>> = {};

    /** What each `driverPromise` key was built for, so a swap can rebuild it. */
    const driverSpecs: Record<string, { dataSource: string, preAggregations: boolean }> = {};

    const ctxRef = { current: context };

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

    const initialDriverVersion = this.options.driverVersion
      ? await this.resolveDriverVersion(context)
      : undefined;

    const state: DriverVersionState = {
      orchestratorId,
      version: initialDriverVersion,
      ctxRef,
      swapDrivers: null,
    };

    const buildDriver = async (
      driverContext: RequestContext,
      dataSource: string,
      preAggregations: boolean,
      inUse?: Set<BaseDriver>,
    ): Promise<BaseDriver> => {
      let driver: BaseDriver | null = null;

      try {
        driver = await this.resolveDriver(
          {
            ...driverContext,
            dataSource,
            preAggregations,
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
        // A factory that caches its instances can hand back a driver still serving queries
        if (driver && !inUse?.has(driver)) {
          await driver.release();
        }

        throw e;
      }
    };

    /**
     * Rebuilds every cached driver from `swapContext` and installs all of them at
     * once, or none: on failure the old drivers and version stay.
     */
    const swapDrivers = async (swapContext: RequestContext, swapVersion: string) => {
      const replacements = new Map<Promise<BaseDriver>, { previous: BaseDriver | null, next: BaseDriver | null }>();
      const inUse = new Set<BaseDriver>();

      const unswapped = () => {
        const firstKeys = new Map<Promise<BaseDriver>, string>();

        Object.keys(driverPromise).forEach((key) => {
          const promise = driverPromise[key];

          if (promise && !replacements.has(promise) && !firstKeys.has(promise)) {
            firstKeys.set(promise, key);
          }
        });

        return firstKeys;
      };

      try {
        // Repeated because a driver built lazily during the swap used the old context
        for (let pending = unswapped(); pending.size; pending = unswapped()) {
          const promises = [...pending.keys()];
          const previous = await Promise.all(promises.map((promise) => promise.catch(() => null)));

          previous.forEach((driver) => driver && inUse.add(driver));

          const built = await Promise.allSettled(promises.map((promise, i) => {
            // Its own build failed and dropped it from the cache
            if (!previous[i]) {
              return null;
            }

            const spec = driverSpecs[pending.get(promise)];

            return buildDriver(swapContext, spec.dataSource, spec.preAggregations, inUse);
          }));

          built.forEach((result, i) => {
            if (result.status === 'fulfilled') {
              replacements.set(promises[i], { previous: previous[i], next: result.value });
            }
          });

          const failure = built.find((result): result is PromiseRejectedResult => result.status === 'rejected');

          if (failure) {
            throw failure.reason;
          }
        }
      } catch (e) {
        await Promise.all(
          [...new Set([...replacements.values()].map(({ next }) => next))]
            .filter((next) => next && !inUse.has(next))
            .map((next) => this.releaseDriverSafely(next)),
        );

        throw e;
      }

      // Synchronous from here on, so no request sees some keys swapped and others not
      const installed = new Set<BaseDriver>();
      const replaced = new Set<BaseDriver>();
      const discarded = new Set<BaseDriver>();
      const now = Date.now();

      replacements.forEach(({ previous, next }, previousPromise) => {
        const keys = Object.keys(driverPromise).filter((key) => driverPromise[key] === previousPromise);

        if (!next) {
          return;
        }

        if (!keys.length) {
          discarded.add(next);
          return;
        }

        const nextPromise = Promise.resolve(next);

        keys.forEach((key) => {
          driverPromise[key] = nextPromise;
        });

        installed.add(next);
        replaced.add(previous);
      });

      replaced.forEach((driver) => {
        if (!installed.has(driver)) {
          this.scheduleSwappedDriverRelease(driver);
        }
      });

      // Never handed out, so nothing can be using them
      discarded.forEach((driver) => {
        if (!installed.has(driver) && !inUse.has(driver)) {
          this.releaseDriverSafely(driver);
        }
      });

      if (state.lastSwapAt !== undefined && now - state.lastSwapAt < DRIVER_SWAP_PING_PONG_MS) {
        this.logger('Driver Swap Ping-Pong', {
          orchestratorId,
          warning: `Drivers were swapped twice within ${DRIVER_SWAP_PING_PONG_MS / 1000}s. contextToOrchestratorId must give ` +
            'contexts with different driverVersion values different ids, or they keep replacing each other\'s drivers.',
        });
      }

      ctxRef.current = swapContext;
      state.version = swapVersion;
      state.lastSwapAt = now;

      this.logger('Driver Swap', {
        orchestratorId,
        dataSources: Object.keys(driverPromise).filter((key) => driverPromise[key]),
      });
    };

    state.swapDrivers = swapDrivers;

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

        if (preAggregations && hasSeparatePreAggEnv && this.optsHandler.isCustomDriverFactory()) {
          this.logger('Pre-aggregation driver conflict', {
            error: 'Both driverFactory and PRE_AGGREGATIONS env vars are defined. driverFactory will take precedence.',
            dataSource,
          });
        }

        // No separate pre-agg driver needed — share the same promise for both keys
        const keys = !preAggregations && !hasSeparatePreAggEnv
          ? [factoryKey, `${dataSource}@pre_agg`]
          : [factoryKey];
        const spec = { dataSource, preAggregations: usePreAgg || false };

        const pending = buildDriver(ctxRef.current, dataSource, spec.preAggregations)
          .catch((e) => {
            keys.forEach((key) => {
              // A swap may have replaced it meanwhile
              if (driverPromise[key] === pending) {
                driverPromise[key] = null;
              }
            });

            throw e;
          });

        keys.forEach((key) => {
          driverPromise[key] = pending;
          driverSpecs[key] = spec;
        });

        return pending;
      },
      {
        // Built from the creating context and never swapped by `driverVersion`
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

    this.driverVersionStates.set(orchestratorApi, state);
    this.orchestratorStorage.set(orchestratorId, orchestratorApi);

    return orchestratorApi;
  }

  protected createCompilerApi(repository: SchemaFileRepository, options: Record<string, any> = {}) {
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
      onCompiled: options.onCompiled,
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
  public handleScheduledRefreshInterval = async (options: ScheduledRefreshOptions) => {
    const allContexts = await this.options.scheduledRefreshContexts();
    if (allContexts.length < 1) {
      this.logger('Refresh Scheduler Error', {
        error: 'At least one context should be returned by scheduledRefreshContexts'
      });
    }

    const contexts: RequestContext[] = [];

    for (const allContext of allContexts) {
      const resContext = this.migrateBackgroundContext(allContext);
      const res = await this.contextAcceptor.shouldAccept(resContext);

      if (res.accepted) {
        contexts.push(resContext || <RequestContext>{});
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
      return CubejsServerCore.createDriver(type, opts);
    }
  }

  public async testConnections() {
    return this.orchestratorStorage.testConnections();
  }

  public async releaseConnections() {
    await this.orchestratorStorage.releaseConnections();
    await this.releaseSwappedDrivers();

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

    await this.orchestratorStorage.releaseConnections();
    await this.releaseSwappedDrivers();
  }
}
