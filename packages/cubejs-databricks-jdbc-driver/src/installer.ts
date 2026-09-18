import fs from 'fs';
import path from 'path';
import { downloadAndExtractFile } from '@cubejs-backend/shared';

export const DRIVER_VERSION = '3.4.2';
export const JDBC_DRIVER_JAR_NAME = `databricks-jdbc-${DRIVER_VERSION}.jar`;

/**
 * Databricks' open-source JDBC driver, which replaced the legacy Simba driver.
 * Requires a Java Runtime Environment (JRE) 11.0 or above.
 * https://docs.databricks.com/aws/en/integrations/jdbc/oss
 */
export async function downloadJDBCDriver(): Promise<string | null> {
  console.log(`Downloading ${JDBC_DRIVER_JAR_NAME}`);

  await downloadAndExtractFile(
    `https://repo1.maven.org/maven2/com/databricks/databricks-jdbc/${DRIVER_VERSION}/${JDBC_DRIVER_JAR_NAME}`,
    {
      showProgress: true,
      cwd: path.resolve(path.join(__dirname, '..', 'download')),
      skipExtract: true,
      dstFileName: JDBC_DRIVER_JAR_NAME,
    }
  );

  console.log(`Release notes: https://mvnrepository.com/artifact/com.databricks/databricks-jdbc/${DRIVER_VERSION}`);

  return path.resolve(path.join(__dirname, '..', 'download', JDBC_DRIVER_JAR_NAME));
}

async function fileExistsOr(
  fsPath: string,
  fn: () => Promise<string>,
): Promise<string> {
  if (fs.existsSync(fsPath)) {
    return fsPath;
  }
  return fn();
}

export async function resolveJDBCDriver(): Promise<string> {
  return fileExistsOr(
    path.join(process.cwd(), JDBC_DRIVER_JAR_NAME),
    async () => fileExistsOr(
      path.join(__dirname, '..', 'download', JDBC_DRIVER_JAR_NAME),
      async () => {
        const pathOrNull = await downloadJDBCDriver();
        if (pathOrNull) {
          return pathOrNull;
        }
        throw new Error(
          `Please download and place ${JDBC_DRIVER_JAR_NAME} inside your ` +
          'project directory'
        );
      }
    )
  );
}
