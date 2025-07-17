/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `ArrowFlightSQLJdbcDriver`.
 */

import { JDBCDriver, JDBCDriverConfiguration, } from '@cubejs-backend/jdbc-driver';
import { assertDataSource, getEnv } from '@cubejs-backend/shared';
import { resolveJDBCDriver } from './driver';

/**
 * ArrowFlightSQLJdbcDriver driver class.
 */
export class ArrowFlightSQLJdbcDriver extends JDBCDriver {
  protected readonly config: JDBCDriverConfiguration;

  public constructor(
    conf: Partial<JDBCDriverConfiguration> & {
      /**
       * Data source name.
       */
      dataSource?: string,

      /**
       * Max pool size value for the [cube]<-->[db] pool.
       */
      maxPoolSize?: number,

      /**
       * Time to wait for a response from a connection after a validation
       * request before determining it as not valid. Default - 10000 ms.
       */
      testConnectionTimeout?: number,
    } = {},
  ) {
    const dataSource =
      conf.dataSource ||
      assertDataSource('default');

    const url: string =
      conf?.url ||
      getEnv('jdbcUrl', { dataSource }) ||
      `jdbc:arrow-flight-sql://${process.env.CUBEJS_DB_HOST}:${process.env.CUBEJS_DB_PORT || '50051'}/${process.env.CUBEJS_DB_NAME}`;

    const config: JDBCDriverConfiguration = {
      ...conf,
      database: getEnv('dbName', { required: false, dataSource }),
      dbType: 'flightsql-jdbc',
      url,
      drivername: 'org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver',
      customClassPath: undefined,
      properties: {
        user: '', // user is always empty but part of jdbc API
        password: process.env.CUBEJS_FLIGHT_SQL_API_KEY || '',
        useEncryption: process.env.CUBEJS_FLIGHT_SQL_USE_ENCRYPTION || 'false',
        disableCertificateVerification: process.env.CUBEJS_FLIGHT_SQL_DISABLE_CERT_VERIFICATION || 'true',
      }
    };

    super(config);
    this.config = config;
  }

  protected override async getCustomClassPath() {
    return resolveJDBCDriver();
  }
}
