import path from 'path';
import { downloadAndExtractFile, getEnv } from '@cubejs-backend/shared';

function acceptedByEnv() {
  const acceptStatus = getEnv('databrickAcceptPolicy');
  if (acceptStatus) {
    console.log('You accepted Terms & Conditions for JDBC driver from DataBricks by CUBEJS_DB_DATABRICKS_ACCEPT_POLICY');
  }

  if (acceptStatus === false) {
    console.log('You declined Terms & Conditions for JDBC driver from DataBricks by CUBEJS_DB_DATABRICKS_ACCEPT_POLICY');
    console.log('Installation will be skipped');
  }

  return acceptStatus;
}

/**
 * In the beginning of 2025 Databricks released their open-source version of JDBC driver and encourage
 * all users to migrate to it as company plans to focus on improving and evolving it over legacy simba driver.
 * More info about OSS Driver could be found at https://docs.databricks.com/aws/en/integrations/jdbc/oss
 * As of March 2025 To use the Databricks JDBC Driver (OSS), the following requirements must be met:
 * Java Runtime Environment (JRE) 11.0 or above. CI testing is supported on JRE 11, 17, and 21.
 */
export async function downloadJDBCDriver(): Promise<string | null> {
  const driverAccepted = acceptedByEnv();

  if (driverAccepted) {
    console.log('Downloading databricks-jdbc-1.0.2-oss.jar');

    await downloadAndExtractFile(
      'https://repo1.maven.org/maven2/com/databricks/databricks-jdbc/1.0.2-oss/databricks-jdbc-1.0.2-oss.jar',
      {
        showProgress: true,
        cwd: path.resolve(path.join(__dirname, '..', 'download')),
        noExtract: true,
      }
    );

    console.log('Release notes: https://mvnrepository.com/artifact/com.databricks/databricks-jdbc/1.0.2-oss');

    return path.resolve(path.join(__dirname, '..', 'download', 'databricks-jdbc-1.0.2-oss.jar'));
  }

  return null;
}
