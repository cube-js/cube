import { PostgresDriver, PostgresDriverConfiguration } from '@cubejs-backend/postgres-driver';
import { PoolClient } from 'pg';

export class CrateDriver extends PostgresDriver<PostgresDriverConfiguration> {
  protected async prepareConnection(
    conn: PoolClient,
    options: { executionTimeout: number } = {
      executionTimeout: this.config.executionTimeout ? <number>(this.config.executionTimeout) * 1000 : 600000
    }
  ) {
    // Not supported by Crate yet... https://github.com/crate/crate/issues/12356
    // await conn.query(`SET TIME ZONE '${this.config.storeTimezone || 'UTC'}'`);
    // await conn.query(`SET statement_timeout TO ${options.executionTimeout}`);

    await this.loadUserDefinedTypes(conn);
  }
}
