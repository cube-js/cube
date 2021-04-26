import { CheckAuthFn, CheckAuthMiddlewareFn, ExtendContextFn, QueryTransformerFn, JWTOptions } from '@cubejs-backend/api-gateway';
import { BaseDriver, RedisPoolOptions } from '@cubejs-backend/query-orchestrator';
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

export type UserBackgroundContext = {
  // @deprecated Renamed to securityContext, please use securityContext.
  authInfo?: any;
  securityContext: any;
};

export interface DriverContext extends RequestContext {
  dataSource: string;
}

export interface DialectContext extends DriverContext {
  dbType: string;
}

export interface DriverFactory {}

export type DatabaseType =
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
  | 'sqlite';

export type ContextToAppIdFn = (context: RequestContext) => string;

export type OrchestratorOptionsFn = (context: RequestContext) => OrchestratorOptions;

export type PreAggregationsSchemaFn = (context: RequestContext) => string;

export type ExternalDbTypeFn = (context: RequestContext) => DatabaseType;

export type ExternalDriverFactoryFn = (context: RequestContext) => Promise<BaseDriver>|BaseDriver;

export type ExternalDialectFactoryFn = (context: RequestContext) => BaseQuery;

export type DbTypeFn = (context: RequestContext) => DatabaseType;

export type LoggerFn = (msg: string, params: any) => void;

export interface CreateOptions {
  dbType?: DatabaseType | DbTypeFn;
  externalDbType?: DatabaseType | ExternalDbTypeFn;
  schemaPath?: string;
  basePath?: string;
  devServer?: boolean;
  apiSecret?: string;
  logger?: LoggerFn;
  driverFactory?: (context: DriverContext) => Promise<BaseDriver>|BaseDriver;
  dialectFactory?: (context: DialectContext) => BaseQuery;
  externalDriverFactory?: ExternalDriverFactoryFn;
  externalDialectFactory?: ExternalDialectFactoryFn;
  contextToAppId?: ContextToAppIdFn;
  contextToOrchestratorId?: (context: RequestContext) => string;
  repositoryFactory?: (context: RequestContext) => SchemaFileRepository;
  checkAuthMiddleware?: CheckAuthMiddlewareFn;
  checkAuth?: CheckAuthFn;
  jwt?: JWTOptions;
  queryTransformer?: QueryTransformerFn;
  preAggregationsSchema?: string | PreAggregationsSchemaFn;
  schemaVersion?: (context: RequestContext) => string;
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
}
