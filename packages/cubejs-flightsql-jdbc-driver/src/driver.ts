/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * File overview: handle JDBC driver retrieval, including lazy downloading and caching
 */

import fs from 'fs';
import path from 'path';
import { downloadAndExtractFile } from '@cubejs-backend/shared';

async function fileExistsOr(
  fsPath: string,
  fn: () => Promise<string>
): Promise<string> {
  if (fs.existsSync(fsPath)) {
    return fsPath;
  }
  return fn();
}

function driverVersion() {
  return process.env.ARROW_FLIGHT_SQL_DRIVER_VERSION || '18.3.0';
}

async function downloadJDBCDriver(): Promise<string> {
  const version = driverVersion();
  try {
    await downloadAndExtractFile(
      `https://repo1.maven.org/maven2/org/apache/arrow/flight-sql-jdbc-driver/${version}/flight-sql-jdbc-driver-${version}.jar`,
      {
        showProgress: false,
        cwd: path.resolve(path.join(__dirname, '..', 'download')),
        skipExtract: true,
        dstFileName: `flight-sql-jdbc-driver-${version}.jar`,
      }
    );
    const driverPath = path.resolve(
      path.join(
        __dirname,
        '..',
        'download',
        `flight-sql-jdbc-driver-${version}.jar`
      )
    );
    console.log(
      `Downloaded org/apache/arrow/flight-sql-jdbc-driver/${version}/flight-sql-jdbc-driver-${version}.jar to ${driverPath}`
    );
    return driverPath;
  } catch (e) {
    console.error(e);
    console.error(`Please download and place flight-sql-jdbc-driver-${version}.jar inside your project directory`);
    throw (e);
  }
}

export async function resolveJDBCDriver(): Promise<string> {
  const version = driverVersion();
  return fileExistsOr(
    path.join(process.cwd(), `flight-sql-jdbc-driver-${version}.jar`),
    async () => fileExistsOr(
      path.join(
        __dirname,
        '..',
        'download',
        `flight-sql-jdbc-driver-${version}.jar`
      ),
      downloadJDBCDriver
    )
  );
}
