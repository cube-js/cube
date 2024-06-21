import { BaseDriver } from "@cubejs-backend/query-orchestrator";
import { config } from "mssql";

declare module "@cubejs-backend/mssql-driver" {
  export default class MSSqlDriver extends BaseDriver {
    constructor(options?: config);
    release(): Promise<void>
  }
}
