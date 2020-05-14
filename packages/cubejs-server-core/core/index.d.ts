import {
  Request as ExpressRequest,
  Response as ExpressResponse,
  NextFunction as ExpressNextFunction
} from "express";
import { NormalizedQuery as Query } from "@cubejs-backend/api-gateway";

declare module "@cubejs-backend/server-core" {
  export function create(options?: CreateOptions): any;

  export interface CreateOptions {
    dbType?: DatabaseType | ((context: RequestContext) => DatabaseType);
    externalDbType?: DatabaseType | ((context: RequestContext) => DatabaseType);
    schemaPath?: string;
    basePath?: string;
    devServer?: boolean;
    apiSecret?: string;
    logger?: (msg: string, params: any) => void;
    driverFactory?: (context: DriverContext) => any;
    externalDriverFactory?: (context: RequestContext) => any;
    contextToAppId?: (context: RequestContext) => string;
    contextToDataSourceId?: (context: RequestContext) => string;
    repositoryFactory?: (context: RequestContext) => SchemaFileRepository;
    checkAuthMiddleware?: (
      req: ExpressRequest,
      res: ExpressResponse,
      next: ExpressNextFunction
    ) => void;
    queryTransformer?: (query: Query, context: RequestContext) => Query;
    preAggregationsSchema?: String | ((context: RequestContext) => string);
    schemaVersion?: (context: RequestContext) => string;
    extendContext?: (req: ExpressRequest) => any;
    scheduledRefreshTimer?: boolean | number;
    compilerCacheSize?: number;
    maxCompilerCacheKeepAlive?: number;
    updateCompilerCacheKeepAlive?: boolean;
    telemetry?: boolean;
    allowUngroupedWithoutPrimaryKey?: boolean;
    orchestratorOptions?: OrchestratorOptions;
  }

  export interface OrchestratorOptions {
    redisPrefix?: string;
    queryCacheOptions?: QueryCacheOptions;
    preAggregationsOptions?: PreAggregationsOptions;
  }

  export interface QueryCacheOptions {
    refreshKeyRenewalThreshold?: number;
    backgroundRenew?: boolean;
    queueOptions?: QueueOptions;
  }

  export interface PreAggregationsOptions {
    queueOptions?: QueueOptions;
  }

  export interface QueueOptions {
    concurrency?: number;
    continueWaitTimeout?: number;
    executionTimeout?: number;
    orphanedTimeout?: number;
    heartBeatInterval?: number;
  }

  export interface RequestContext {
    authInfo: any;
    requestId: string;
  }

  export interface DriverContext extends RequestContext {
    dataSource: string;
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
    | "athena"
    | "bigquery"
    | "clickhouse"
    | "jdbc"
    | "hive"
    | "mongobi"
    | "mssql"
    | "mysql"
    | "elasticsearch"
    | "awselasticsearch"
    | "oracle"
    | "postgres"
    | "prestodb"
    | "redshift"
    | "snowflake"
    | "sqlite";
}
