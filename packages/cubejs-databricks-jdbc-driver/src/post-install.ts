import 'source-map-support/register';

import { displayCLIError } from '@cubejs-backend/shared';

import fs from 'fs';
import path from 'path';
import { downloadJDBCDriver } from './installer';

(async () => {
  try {
    if (!fs.existsSync(path.join(__dirname, '..', 'download', 'SparkJDBC42.jar'))) {
      await downloadJDBCDriver();
    }
  } catch (e) {
    await displayCLIError(e, 'Cube.js Databricks JDBC Installer');
  }
})();
