import 'source-map-support/register';

import { displayCLIError } from '@cubejs-backend/shared';
import { resolveJDBCDriver } from './helpers';

(async () => {
  try {
    await resolveJDBCDriver();
  } catch (e: any) {
    await displayCLIError(e, 'Cube Databricks JDBC Installer');
  }
})();
