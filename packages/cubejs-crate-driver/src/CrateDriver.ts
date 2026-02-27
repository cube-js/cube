/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `CrateDriver` and related types declaration.
 */

import { PostgresDriver, PgClient } from '@cubejs-backend/postgres-driver';

export class CrateDriver extends PostgresDriver {
  protected async prepareConnection(conn: PgClient, _options: any) {
    // Not supported by Crate yet... https://github.com/crate/crate/issues/12356
    // await conn.query(`SET TIME ZONE '${this.config.storeTimezone || 'UTC'}'`);
    // await conn.query(`SET statement_timeout TO ${options.executionTimeout}`);

    await this.loadUserDefinedTypes(conn);
  }
}
