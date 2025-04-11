import path from 'path';
import { downloadAndExtractFile, getEnv } from '@cubejs-backend/shared';

export const OSS_DRIVER_VERSION = '1.0.2';

/**
 * In the beginning of 2025 Databricks released their open-source version of JDBC driver and encourage
 * all users to migrate to it as company plans to focus on improving and evolving it over legacy simba driver.
 * More info about OSS Driver could be found at https://docs.databricks.com/aws/en/integrations/jdbc/oss
 * As of March 2025 To use the Databricks JDBC Driver (OSS), the following requirements must be met:
 * Java Runtime Environment (JRE) 11.0 or above. CI testing is supported on JRE 11, 17, and 21.
 */
export async function downloadJDBCDriver(): Promise<string | null> {
  // TODO: Just to throw a console warning that this ENV is obsolete and could be safely removed
  getEnv('databrickAcceptPolicy');

  console.log(`Downloading databricks-jdbc-${OSS_DRIVER_VERSION}-oss.jar`);

  await downloadAndExtractFile(
    `https://repo1.maven.org/maven2/com/databricks/databricks-jdbc/${OSS_DRIVER_VERSION}-oss/databricks-jdbc-${OSS_DRIVER_VERSION}-oss.jar`,
    {
      showProgress: true,
      cwd: path.resolve(path.join(__dirname, '..', 'download')),
      skipExtract: true,
      dstFileName: `databricks-jdbc-${OSS_DRIVER_VERSION}-oss.jar`,
    }
  );

  console.log(`Release notes: https://mvnrepository.com/artifact/com.databricks/databricks-jdbc/${OSS_DRIVER_VERSION}-oss`);

  return path.resolve(path.join(__dirname, '..', 'download', `databricks-jdbc-${OSS_DRIVER_VERSION}-oss.jar`));
}
