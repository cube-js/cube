import type { RDSDataClientConfig } from '@aws-sdk/client-rds-data';

declare module '@cubejs-backend/mysql-aurora-serverless-driver' {
  export interface ConnectionOptions {
    secretArn?: string,
    resourceArn?: string,
    database?: string,
    options?: RDSDataClientConfig
  }

  export default class AuroraServerlessMySqlDriver {
    constructor(options?: ConnectionOptions);

    positionBindings(sql: string): string;
  }
}
