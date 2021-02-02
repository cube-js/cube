import { BaseDriver } from "@cubejs-backend/query-orchestrator";
import { ConnectionOptions } from "mysql2";

declare module "@cubejs-backend/mongobi-driver" {
  export default class MongoBIDriver extends BaseDriver {
    constructor(options?: ConnectionOptions);
    release(): Promise<void>
  }
}
