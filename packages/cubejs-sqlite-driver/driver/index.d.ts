import { BaseDriver } from "@cubejs-backend/query-orchestrator";
declare module "@cubejs-backend/sqlite-driver" {
  interface SqliteOptions {
    database: string;
  }

  export default class SqliteDriver extends BaseDriver {
    constructor(options?: SqliteOptions);
    release(): Promise<void>
  }
}
