import 'source-map-support/register';

import path from 'path';
import { displayCLIError, displayCLIWarning, downloadAndExtractFile } from '@cubejs-backend/shared';
import inquirer from 'inquirer';

(async () => {
  try {
    if (process.stdout.isTTY) {
      console.log('Databricks driver is using JDBC driver from Data Bricks');
      console.log('By downloading the driver, you agree to the Terms & Conditions');
      console.log('https://databricks.com/jdbc-odbc-driver-license');
      console.log('More info: https://databricks.com/spark/jdbc-drivers-download');

      const { licenseAccepted } = await inquirer.prompt([{
        type: 'confirm',
        name: 'licenseAccepted',
        message: 'You read & agree to the Terms & Conditions',
      }]);
      if (licenseAccepted) {
        console.log('Downloading SimbaSparkJDBC42-2.6.17.1021');

        await downloadAndExtractFile(
          'https://databricks-bi-artifacts.s3.us-east-2.amazonaws.com/simbaspark-drivers/jdbc/2.6.17/SimbaSparkJDBC42-2.6.17.1021.zip',
          {
            showProgress: true,
            cwd: path.resolve(path.join(__dirname, '..', '..', 'download')),
          }
        );

        console.log('Release notes: https://databricks-bi-artifacts.s3.us-east-2.amazonaws.com/simbaspark-drivers/jdbc/2.6.17/docs/release-notes.txt');
      } else {
        displayCLIWarning(
          'Driver downloaded will be skipped, you didn\'t accept Terms & Conditions'
        );
      }
    } else {
      displayCLIWarning(
        'Driver downloaded will be skipped, terminal is not interactive'
      );
    }
  } catch (e) {
    await displayCLIError(e, 'Cube.js Databricks JDBC Installer');
  }
})();
