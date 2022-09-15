import { displayCLIError } from '@cubejs-backend/shared';

import { downloadJDBCDriver } from './installer';

(async () => {
  try {
    await downloadJDBCDriver(true);
  } catch (e: any) {
    await displayCLIError(e, 'Cube.js Teradata JDBC Installer');
  }
})();
