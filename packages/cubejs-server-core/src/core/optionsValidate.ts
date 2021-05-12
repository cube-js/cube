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
  driverFactory: Joi.func(),
  externalDriverFactory: Joi.func(),
  contextToAppId: Joi.func(),
  contextToDataSourceId: Joi.func(),
  contextToOrchestratorId: Joi.func(),
  repositoryFactory: Joi.func(),
  checkAuth: Joi.func(),
  checkAuthMiddleware: Joi.func(),
  jwt: jwtOptions,
  queryTransformer: Joi.func(),
  preAggregationsSchema: Joi.alternatives().try(
    Joi.string(),
    Joi.func()
  ),
  schemaVersion: Joi.func(),
  extendContext: Joi.func(),
  scheduledRefreshTimer: Joi.alternatives().try(
    Joi.boolean(),
    Joi.number().min(0).integer()
  ),
  compilerCacheSize: Joi.number().min(0).integer(),
  maxCompilerCacheKeepAlive: Joi.number().min(0).integer(),
  scheduledRefreshConcurrency: Joi.number().min(1).integer(),
  updateCompilerCacheKeepAlive: Joi.boolean(),
  telemetry: Joi.boolean(),
  allowUngroupedWithoutPrimaryKey: Joi.boolean(),
  orchestratorOptions: Joi.alternatives().try(
    Joi.func(),
    Joi.object().keys({
      redisPrefix: Joi.string().allow(''),
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
      },
      rollupOnlyMode: Joi.boolean()
    })
  ),
  allowJsDuplicatePropsInSchema: Joi.boolean(),
  scheduledRefreshContexts: Joi.func(),
  sqlCache: Joi.boolean(),
  livePreview: Joi.boolean(),
  // Additional system flags
  serverless: Joi.boolean(),
});

export default (options: any) => {
  const { error } = Joi.validate(options, schemaOptions, { abortEarly: false, });
  if (error) {
    throw new Error(`Invalid cube-server-core options: ${error.message || error.toString()}`);
  }
};
