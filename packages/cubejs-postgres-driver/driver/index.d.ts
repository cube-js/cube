import { BaseDriver } from "@cubejs-backend/query-orchestrator";
import { PoolConfig } from "pg";

declare module "@cubejs-backend/postgres-driver" {
  export default class PostgresDriver extends BaseDriver {
    constructor(options?: PoolConfig);
    release(): Promise<void>
  }
}
