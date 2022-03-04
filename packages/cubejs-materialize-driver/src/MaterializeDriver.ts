import { PostgresDriver, PostgresDriverConfiguration } from '@cubejs-backend/postgres-driver';
import { PoolClient } from 'pg';

export class MaterializeDriver extends PostgresDriver {
  public constructor(options: PostgresDriverConfiguration) {
    super(options);
  }

  protected async prepareConnection(
    conn: PoolClient,
    options: { executionTimeout: number } = {
      executionTimeout: this.config.executionTimeout ? <number>this.config.executionTimeout * 1000 : 600000,
    }
  ) {
    await conn.query(`SET TIME ZONE '${this.config.storeTimezone || 'UTC'}'`);
    // Support for statement_timeout is still pending. https://github.com/MaterializeInc/materialize/issues/10390
    // await conn.query(`SET statement_timeout TO ${options.executionTimeout}`);

    await this.loadUserDefinedTypes(conn);
  }
}
