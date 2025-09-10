/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `ArrowFlightSQLJdbcDriver`.
 */

import { JDBCDriver, JDBCDriverConfiguration, } from '@cubejs-backend/jdbc-driver';
import fs from 'fs';
import path from 'path';
import { assertDataSource, downloadAndExtractFile, getEnv } from '@cubejs-backend/shared';

export const OSS_DRIVER_VERSION = '18.3.0';

async function fileExistsOr(
  fsPath: string,
  fn: () => Promise<string>,
): Promise<string> {
  if (fs.existsSync(fsPath)) {
    return fsPath;
  }
  return fn();
}

async function downloadJDBCDriver(): Promise<string | null> {
  await downloadAndExtractFile(
    `https://repo1.maven.org/maven2/org/apache/arrow/flight-sql-jdbc-driver/${OSS_DRIVER_VERSION}/flight-sql-jdbc-driver-${OSS_DRIVER_VERSION}.jar`,
    {
      showProgress: false,
      cwd: path.resolve(path.join(__dirname, '..', 'download')),
      skipExtract: true,
      dstFileName: `flight-sql-jdbc-driver-${OSS_DRIVER_VERSION}.jar`,
    }
  );

  return path.resolve(path.join(__dirname, '..', 'download', `flight-sql-jdbc-driver-${OSS_DRIVER_VERSION}.jar`));
}

export async function resolveJDBCDriver(): Promise<string> {
  return fileExistsOr(
    path.join(process.cwd(), `flight-sql-jdbc-driver-${OSS_DRIVER_VERSION}.jar`),
    async () => fileExistsOr(
      path.join(__dirname, '..', 'download', `flight-sql-jdbc-driver-${OSS_DRIVER_VERSION}.jar`),
      async () => {
        const pathOrNull = await downloadJDBCDriver();
        if (pathOrNull) {
          return pathOrNull;
        }
        throw new Error(
          `Please download and place flight-sql-jdbc-driver-${OSS_DRIVER_VERSION}.jar inside your ` +
          'project directory'
        );
      }
    )
  );
}

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
       * Time to wait for a response from a connection after validation
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
