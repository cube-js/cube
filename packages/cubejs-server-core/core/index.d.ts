declare module '@cubejs-backend/server-core' {
  export function create(options?: CreateOptions): any;

  export function createDriver(dbType: DatabaseType): DriverFactory;

  export interface CreateOptions {
    apiSecret?: string;
    basePath?: string;
    checkAuthMiddleware?: (req: any, res: any, next: any) => any;
    queryTransformer?: () => (query: any, context: any) => any;
    contextToAppId?: any;
    contextToDataSourceId?: (context: any) => string;
    devServer?: boolean;
    dbType?: DatabaseType;
    driverFactory?: DriverFactory;
    externalDriverFactory?: DriverFactory;
    logger?: (msg: string, params: any) => void;
    orchestratorOptions?: any;
    preAggregationsSchema?: string | (() => string)
    repositoryFactory?: any;
    schemaPath?: string;
    compilerCacheSize?: number;
    maxCompilerCacheKeepAlive?: number;
    updateCompilerCacheKeepAlive?: boolean;
  }

  export interface DriverFactory {
  }

  export type DatabaseType = 'athena' | 'bigquery' | 'clickhouse' | 'jdbc' | 'mongobi' | 'mssql' | 'mysql' | 'postgres' | 'redshift';
}
