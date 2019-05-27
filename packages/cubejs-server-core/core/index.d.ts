declare module '@cubejs-backend/server-core' {
  export function create(options?: CreateOptions): any;

  export function createDriver(dbType: DatabaseType): DriverFactory;

  export interface CreateOptions {
    apiSecret?: string;
    basePath?: string;
    checkAuthMiddleware?: (req: any, res: any, next: any) => any;
    contextToAppId?: any;
    devServer?: boolean;
    dbType?: DatabaseType;
    driverFactory?: DriverFactory;
    externalDriverFactory?: DriverFactory;
    logger?: (msg: string, params: any) => void;
    orchestratorOptions?: any;
    repositoryFactory?: any;
    schemaPath?: string;
  }

  export interface DriverFactory {
  }

  export type DatabaseType = 'athena' | 'bigquery' | 'clickhouse' | 'jdbc' | 'mongobi' | 'mssql' | 'mysql' | 'postgres' | 'redshift';
}
