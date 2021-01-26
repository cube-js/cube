import { Request as ExpressRequest } from 'express';
import { CheckAuthFn, CheckAuthMiddlewareFn, QueryTransformerFn } from '@cubejs-backend/api-gateway';
import { RedisPoolOptions } from '@cubejs-backend/query-orchestrator';

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
}

export interface DriverContext extends RequestContext {
  dataSource: string;
}

export interface DialectContext extends DriverContext {
  dbType: string;
}

export interface FileContent {
  fileName: string;
  content: string;
}

export interface SchemaFileRepository {
  dataSchemaFiles: () => Promise<FileContent[]>;
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

export type DbTypeFn = (context: RequestContext) => DatabaseType;

export interface CreateOptions {
  dbType?: DatabaseType | DbTypeFn;
  externalDbType?: DatabaseType | ExternalDbTypeFn;
  schemaPath?: string;
  basePath?: string;
  devServer?: boolean;
  apiSecret?: string;
  logger?: (msg: string, params: any) => void;
  driverFactory?: (context: DriverContext) => any;
  dialectFactory?: (context: DialectContext) => any;
  externalDriverFactory?: (context: RequestContext) => any;
  externalDialectFactory?: (context: RequestContext) => any;
  contextToAppId?: ContextToAppIdFn;
  contextToOrchestratorId?: (context: RequestContext) => string;
  repositoryFactory?: (context: RequestContext) => SchemaFileRepository;
  checkAuthMiddleware?: CheckAuthMiddlewareFn;
  checkAuth?: CheckAuthFn;
  queryTransformer?: QueryTransformerFn;
  preAggregationsSchema?: string | PreAggregationsSchemaFn;
  schemaVersion?: (context: RequestContext) => string;
  extendContext?: (req: ExpressRequest) => any;
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
}
