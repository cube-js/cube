import { BaseDriver } from "@cubejs-backend/query-orchestrator";
import { ConnectionConfig } from "mysql";

declare module "@cubejs-backend/mysql-driver" {
  export default class MySqlDriver extends BaseDriver {
    constructor(options?: ConnectionConfig);
    release(): Promise<void>
  }
}
