import { PostgresDriver } from '@cubejs-backend/postgres-driver';
import { PoolClient } from 'pg';

export class CrateDriver extends PostgresDriver {
  protected async prepareConnection(conn: PoolClient, _options: any) {
    // Not supported by Crate yet... https://github.com/crate/crate/issues/12356
    // await conn.query(`SET TIME ZONE '${this.config.storeTimezone || 'UTC'}'`);
    // await conn.query(`SET statement_timeout TO ${options.executionTimeout}`);

    await this.loadUserDefinedTypes(conn);
  }
}
