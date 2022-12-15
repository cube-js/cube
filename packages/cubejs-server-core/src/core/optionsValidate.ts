import Joi from '@hapi/joi';
import DriverDependencies from './DriverDependencies';

const schemaQueueOptions = Joi.object().keys({
  concurrency: Joi.number().min(1).integer(),
  continueWaitTimeout: Joi.number().min(0).integer(),
  executionTimeout: Joi.number().min(0).integer(),
  orphanedTimeout: Joi.number().min(0).integer(),
  heartBeatInterval: Joi.number().min(0).integer(),
  sendProcessMessageFn: Joi.func(),
  sendCancelMessageFn: Joi.func(),
});

const jwtOptions = Joi.object().keys({
  // JWK options
  jwkRetry: Joi.number().min(1).max(5).integer(),
  jwkDefaultExpire: Joi.number().min(0),
  jwkUrl: Joi.alternatives().try(
    Joi.string(),
    Joi.func()
  ),
  jwkRefetchWindow: Joi.number().min(0),
  // JWT options
  key: Joi.string(),
  algorithms: Joi.array().items(Joi.string()),
  issuer: Joi.array().items(Joi.string()),
  audience: Joi.string(),
  subject: Joi.string(),
  claimsNamespace: Joi.string(),
});

const dbTypes = Joi.alternatives().try(
  Joi.string().valid(...Object.keys(DriverDependencies)),
  Joi.func()
);

const schemaOptions = Joi.object().keys({
  // server CreateOptions
  initApp: Joi.func(),
  webSockets: Joi.boolean(),
  http: Joi.object().keys({
    cors: Joi.object(),
  }),
  gracefulShutdown: Joi.number().min(0).integer(),
  // Additional from WebSocketServerOptions
  processSubscriptionsInterval: Joi.number(),
  webSocketsBasePath: Joi.string(),
  // server-core CoreCreateOptions
  dbType: dbTypes,
  externalDbType: dbTypes,
  schemaPath: Joi.string(),
  basePath: Joi.string(),
  devServer: Joi.boolean(),
  apiSecret: Joi.string(),
  logger: Joi.func(),
  // source
  dialectFactory: Joi.func(),
  driverFactory: Joi.func(),
  // external
  externalDialectFactory: Joi.func(),
  externalDriverFactory: Joi.func(),
  //
  cacheAndQueueDriver: Joi.string().valid('redis', 'memory'),
  contextToAppId: Joi.func(),
  contextToOrchestratorId: Joi.func(),
  contextToDataSourceId: Joi.func(),
  repositoryFactory: Joi.func(),
  checkAuth: Joi.func(),
  checkRestAcl: Joi.func(),
  checkAuthMiddleware: Joi.func(),
  jwt: jwtOptions,
  queryTransformer: Joi.func(),
  queryRewrite: Joi.func(),
  preAggregationsSchema: Joi.alternatives().try(
    Joi.string(),
    Joi.func()
  ),
  schemaVersion: Joi.func(),
  extendContext: Joi.func(),
  // Scheduled refresh
  scheduledRefreshTimer: Joi.alternatives().try(
    Joi.boolean(),
    Joi.number().min(0).integer()
  ),
  scheduledRefreshTimeZones: Joi.array().items(Joi.string()),
  scheduledRefreshContexts: Joi.func(),
  scheduledRefreshConcurrency: Joi.number().min(1).integer(),
  // Compiler cache
  compilerCacheSize: Joi.number().min(0).integer(),
  updateCompilerCacheKeepAlive: Joi.boolean(),
  maxCompilerCacheKeepAlive: Joi.number().min(0).integer(),
  telemetry: Joi.boolean(),
  allowUngroupedWithoutPrimaryKey: Joi.boolean(),
  orchestratorOptions: Joi.alternatives().try(
    Joi.func(),
    Joi.object().keys({
      redisPrefix: Joi.string().allow(''),
      redisPoolOptions: Joi.object().keys({
        poolMin: Joi.number().min(0),
        poolMax: Joi.number().min(0),
        idleTimeoutSeconds: Joi.number().min(0),
        softIdleTimeoutSeconds: Joi.number().min(0),
        createClient: Joi.func(),
        destroyClient: Joi.func(),
        poolOptions: Joi.object().keys({
          maxWaitingClients: Joi.number(),
          testOnBorrow: Joi.bool(),
          testOnReturn: Joi.bool(),
          acquireTimeoutMillis: Joi.number(),
          fifo: Joi.bool(),
          priorityRange: Joi.number(),
          autostart: Joi.bool(),
          evictionRunIntervalMillis: Joi.number().min(0),
          numTestsPerEvictionRun: Joi.number().min(1),
          softIdleTimeoutMillis: Joi.number().min(0),
          idleTimeoutMillis: Joi.number().min(0),
        })
      }),
      continueWaitTimeout: Joi.number().min(0).integer(),
      skipExternalCacheAndQueue: Joi.boolean(),
      queryCacheOptions: Joi.object().keys({
        refreshKeyRenewalThreshold: Joi.number().min(0).integer(),
        backgroundRenew: Joi.boolean(),
        queueOptions: schemaQueueOptions,
        externalQueueOptions: schemaQueueOptions
      }),
      preAggregationsOptions: {
        queueOptions: schemaQueueOptions,
        externalRefresh: Joi.boolean(),
        maxPartitions: Joi.number(),
      },
      rollupOnlyMode: Joi.boolean()
    })
  ),
  allowJsDuplicatePropsInSchema: Joi.boolean(),
  dashboardAppPath: Joi.string(),
  dashboardAppPort: Joi.number(),
  sqlCache: Joi.boolean(),
  livePreview: Joi.boolean(),
  // SQL API
  sqlPort: Joi.number(),
  pgSqlPort: Joi.number(),
  sqlSuperUser: Joi.string(),
  checkSqlAuth: Joi.func(),
  canSwitchSqlUser: Joi.func(),
  sqlUser: Joi.string(),
  sqlPassword: Joi.string(),
  // Additional system flags
  serverless: Joi.boolean(),
  allowNodeRequire: Joi.boolean(),
});

export default (options: any) => {
  const { error } = Joi.validate(options, schemaOptions, { abortEarly: false, });
  if (error) {
    throw new Error(`Invalid cube-server-core options: ${error.message || error.toString()}`);
  }
};
