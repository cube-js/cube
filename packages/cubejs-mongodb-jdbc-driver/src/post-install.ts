import 'source-map-support/register';

import { displayCLIError } from '@cubejs-backend/shared';

import fs from 'fs';
import path from 'path';
import { downloadJDBCDriver } from './installer';

(async () => {
  try {
    if (!fs.existsSync(path.join(__dirname, '..', 'download', 'mongodb-jdbc-2.0.0-all.jar'))) {
      await downloadJDBCDriver();
    }
  } catch (e) {
    await displayCLIError(e, 'Cube Dev Mongo JDBC Installer');
  }
})();
