import 'source-map-support/register';

import { displayCLIError } from '@cubejs-backend/shared';

import { downloadJDBCDriver } from './installer';

(async () => {
  try {
    await downloadJDBCDriver(true);
  } catch (e) {
    await displayCLIError(e, 'Cube.js Databricks JDBC Installer');
  }
})();
