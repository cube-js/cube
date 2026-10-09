import 'source-map-support/register';

import { displayCLIError } from '@cubejs-backend/shared';
import { resolveJDBCDriver } from './installer';

(async () => {
  try {
    await resolveJDBCDriver();
  } catch (e: any) {
    await displayCLIError(e, 'Cube Databricks JDBC Installer');
  }
})();
