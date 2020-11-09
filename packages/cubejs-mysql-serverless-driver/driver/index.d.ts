declare module '@cubejs-backend/mysql-serverless-driver' {
  export default interface ConnectionOptions {
    secretArn?: string
    resourceArn?: string
    database?: string
  }
  export default class ServerlessMySqlDriver {
    constructor(options?: ConnectionOptions);

    positionBindings(sql: string): string;
  }
}
