import {
  CheckAuthFn,
  CheckAuthMiddlewareFn,
  ExtendContextFn,
  JWTOptions,
  UserBackgroundContext,
  QueryRewriteFn,
  CheckSQLAuthFn,
} from '@cubejs-backend/api-gateway';
import { BaseDriver, RedisPoolOptions, CacheAndQueryDriverType } from '@cubejs-backend/query-orchestrator';
import { BaseQuery } from '@cubejs-backend/schema-compiler';
import type { SchemaFileRepository } from './FileRepository';

export interface QueueOptions {
  concurrency?: number;
  continueWaitTimeout?: number;
  executionTimeout?: number;
  orphanedTimeout?: number;
  heartBeatInterval?: number;
}

export interface QueryCacheOptions {
  refreshKeyRenewalThreshold?: number;
  backgroundRenew?: boolean;
  queueOptions?: QueueOptions | ((dataSource: string) => QueueOptions);
  externalQueueOptions?: QueueOptions;
}

export interface PreAggregationsOptions {
  queueOptions?: QueueOptions;
  externalRefresh?: boolean;
}

export interface OrchestratorOptions {
  redisPrefix?: string;
  redisPoolOptions?: RedisPoolOptions;
  queryCacheOptions?: QueryCacheOptions;
  preAggregationsOptions?: PreAggregationsOptions;
  rollupOnlyMode?: boolean;
}

export interface RequestContext {
  // @deprecated Renamed to securityContext, please use securityContext.
  authInfo: any;
  securityContext: any;
  requestId: string;
}

export interface DriverContext extends RequestContext {
  dataSource: string;
}

export interface DialectContext extends DriverContext {
  dbType: string;
}

export interface DriverFactory {}

export type DatabaseType =
  | 'cubestore'
  | 'athena'
  | 'bigquery'
  | 'clickhouse'
  | 'druid'
  | 'jdbc'
  | 'hive'
  | 'mongobi'
  | 'mssql'
  | 'mysql'
  | 'elasticsearch'
  | 'awselasticsearch'
  | 'oracle'
  | 'postgres'
  | 'prestodb'
  | 'redshift'
  | 'snowflake'
  | 'sqlite'
  | 'questdb';

export type ContextToAppIdFn = (context: RequestContext) => string;
export type ContextToOrchestratorIdFn = (context: RequestContext) => string;

export type OrchestratorOptionsFn = (context: RequestContext) => OrchestratorOptions;

export type PreAggregationsSchemaFn = (context: RequestContext) => string;

// internal
export type DbTypeFn = (context: DriverContext) => DatabaseType;
export type DriverFactoryFn = (context: DriverContext) => Promise<BaseDriver> | BaseDriver;
export type DialectFactoryFn = (context: DialectContext) => BaseQuery;

// external
export type ExternalDbTypeFn = (context: RequestContext) => DatabaseType;
export type ExternalDriverFactoryFn = (context: RequestContext) => Promise<BaseDriver> | BaseDriver;
export type ExternalDialectFactoryFn = (context: RequestContext) => BaseQuery;

export type LoggerFn = (msg: string, params: Record<string, any>) => void;

export interface CreateOptions {
  dbType?: DatabaseType | DbTypeFn;
  externalDbType?: DatabaseType | ExternalDbTypeFn;
  schemaPath?: string;
  basePath?: string;
  devServer?: boolean;
  apiSecret?: string;
  logger?: LoggerFn;
  driverFactory?: DriverFactoryFn;
  dialectFactory?: DialectFactoryFn;
  externalDriverFactory?: ExternalDriverFactoryFn;
  externalDialectFactory?: ExternalDialectFactoryFn;
  cacheAndQueueDriver?: CacheAndQueryDriverType;
  contextToAppId?: ContextToAppIdFn;
  contextToOrchestratorId?: ContextToOrchestratorIdFn;
  repositoryFactory?: (context: RequestContext) => SchemaFileRepository;
  checkAuthMiddleware?: CheckAuthMiddlewareFn;
  checkAuth?: CheckAuthFn;
  checkSqlAuth?: CheckSQLAuthFn;
  jwt?: JWTOptions;
  // @deprecated Please use queryRewrite
  queryTransformer?: QueryRewriteFn;
  queryRewrite?: QueryRewriteFn;
  preAggregationsSchema?: string | PreAggregationsSchemaFn;
  schemaVersion?: (context: RequestContext) => string | Promise<string>;
  extendContext?: ExtendContextFn;
  scheduledRefreshTimer?: boolean | number;
  scheduledRefreshTimeZones?: string[];
  scheduledRefreshContexts?: () => Promise<UserBackgroundContext[]>;
  scheduledRefreshConcurrency?: number;
  compilerCacheSize?: number;
  maxCompilerCacheKeepAlive?: number;
  updateCompilerCacheKeepAlive?: boolean;
  telemetry?: boolean;
  allowUngroupedWithoutPrimaryKey?: boolean;
  orchestratorOptions?: OrchestratorOptions | OrchestratorOptionsFn;
  allowJsDuplicatePropsInSchema?: boolean;
  // @deprecated Use contextToOrchestratorId instead.
  contextToDataSourceId?: any;
  dashboardAppPath?: string;
  dashboardAppPort?: number;
  sqlCache?: boolean;
  livePreview?: boolean;
  // Internal flag, that we use to detect serverless env
  serverless?: boolean;
  allowNodeRequire?: boolean;
}

export type SystemOptions = {
  isCubeConfigEmpty: boolean;
};
