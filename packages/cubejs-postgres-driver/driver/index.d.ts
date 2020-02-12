import { PoolConfig } from "pg";

declare module "@cubejs-backend/postgres-driver" {
  class PostgresDriver {
    constructor(options?: PoolConfig);
  }
  export = PostgresDriver;
}
