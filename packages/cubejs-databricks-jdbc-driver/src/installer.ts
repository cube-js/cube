import path from 'path';
import inquirer from 'inquirer';
import { displayCLIWarning, downloadAndExtractFile, getEnv } from '@cubejs-backend/shared';

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

async function cliAcceptVerify() {
  console.log('Databricks driver is using JDBC driver from Data Bricks');
  console.log('By downloading the driver, you agree to the Terms & Conditions');
  console.log('https://databricks.com/jdbc-odbc-driver-license');
  console.log('More info: https://databricks.com/spark/jdbc-drivers-download');

  if (process.stdout.isTTY) {
    const { licenseAccepted } = await inquirer.prompt([{
      type: 'confirm',
      name: 'licenseAccepted',
      message: 'You read & agree to the Terms & Conditions',
    }]);

    return licenseAccepted;
  }

  displayCLIWarning('Your stdout is not interactive, you can accept it via CUBEJS_DB_DATABRICKS_ACCEPT_POLICY=true');

  return false;
}

export async function downloadJDBCDriver(isCli: boolean = false): Promise<string | null> {
  let driverAccepted = acceptedByEnv();

  if (driverAccepted === undefined && isCli) {
    driverAccepted = await cliAcceptVerify();
  }

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
