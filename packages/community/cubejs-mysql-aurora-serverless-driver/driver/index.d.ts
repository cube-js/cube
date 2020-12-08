declare module '@cubejs-backend/mysql-aurora-serverless-driver' {
  export default interface ConnectionOptions {
    secretArn?: string
    resourceArn?: string
    database?: string
  }
  export default class AuroraServerlessMySqlDriver {
    constructor(options?: ConnectionOptions);

    positionBindings(sql: string): string;
  }
}
