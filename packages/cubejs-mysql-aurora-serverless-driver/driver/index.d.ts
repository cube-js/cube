import type { ClientConfiguration } from 'aws-sdk/clients/rdsdataservice';

declare module '@cubejs-backend/mysql-aurora-serverless-driver' {
  export interface ConnectionOptions {
    secretArn?: string,
    resourceArn?: string,
    database?: string,
    options?: ClientConfiguration
  }

  export default class AuroraServerlessMySqlDriver {
    constructor(options?: ConnectionOptions);

    positionBindings(sql: string): string;
  }
}
