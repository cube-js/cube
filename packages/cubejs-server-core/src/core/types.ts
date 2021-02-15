import { CheckAuthFn, CheckAuthMiddlewareFn, ExtendContextFn, QueryTransformerFn, JWTOptions } from '@cubejs-backend/api-gateway';
import { BaseDriver, RedisPoolOptions } from '@cubejs-backend/query-orchestrator';
import { BaseQuery } from '@cubejs-backend/schema-compiler';

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
  driverFactory?: (context: DriverContext) => Promise<BaseDriver>|BaseDriver;
  dialectFactory?: (context: DialectContext) => BaseQuery;
  externalDriverFactory?: (context: RequestContext) => Promise<BaseDriver>|BaseDriver;
  externalDialectFactory?: (context: RequestContext) => BaseQuery;
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
}
