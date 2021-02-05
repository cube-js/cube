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
  jwkRetry: Joi.number().min(1).max(5).integer(),
  jwkUrl: Joi.alternatives().try(
    Joi.string(),
    Joi.func()
  ),
  algorithms: Joi.array().items(Joi.string()),
  issuer: Joi.array().items(Joi.string()),
  audience: Joi.string(),
  subject: Joi.string(),
});

const dbTypes = Joi.alternatives().try(
  Joi.string().valid(...Object.keys(DriverDependencies)),
  Joi.func()
);

const schemaOptions = Joi.object().keys({
  dbType: dbTypes,
  externalDbType: dbTypes,
  schemaPath: Joi.string(),
  basePath: Joi.string(),
  webSocketsBasePath: Joi.string(),
  devServer: Joi.boolean(),
  apiSecret: Joi.string(),
  webSockets: Joi.boolean(),
  processSubscriptionsInterval: Joi.number(),
  initApp: Joi.func(),
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
  scheduledRefreshContexts: Joi.func()
});

export default (options: any) => {
  const { error } = Joi.validate(options, schemaOptions, {
    abortEarly: false,
    // http configuration from server is not a part of server-core, we dont needed to get an error
    allowUnknown: true,
  });
  if (error) {
    throw new Error(`Invalid cube-server-core options: ${error.message || error.toString()}`);
  }
};
