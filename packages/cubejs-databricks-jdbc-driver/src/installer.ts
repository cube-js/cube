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

export async function downloadJDBCDriver(): Promise<string | null> {
  const driverAccepted = acceptedByEnv();

  if (driverAccepted) {
    console.log('Downloading SimbaSparkJDBC42-2.6.17.1021');

    await downloadAndExtractFile(
      'https://databricks-bi-artifacts.s3.us-east-2.amazonaws.com/simbaspark-drivers/jdbc/2.6.17/SimbaSparkJDBC42-2.6.17.1021.zip',
      {
        showProgress: true,
        cwd: path.resolve(path.join(__dirname, '..', 'download')),
      }
    );

    console.log('Release notes: https://databricks-bi-artifacts.s3.us-east-2.amazonaws.com/simbaspark-drivers/jdbc/2.6.17/docs/release-notes.txt');

    return path.resolve(path.join(__dirname, '..', 'download', 'SparkJDBC42.jar'));
  }

  return null;
}
