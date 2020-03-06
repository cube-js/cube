import { PoolConfig } from "pg";

declare module "@cubejs-backend/postgres-driver" {
  export default class PostgresDriver {
    constructor(options?: PoolConfig);
  }
}
