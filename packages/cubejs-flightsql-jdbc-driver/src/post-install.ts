import 'source-map-support/register';

import { displayCLIError } from '@cubejs-backend/shared';
import { resolveJDBCDriver } from './driver';

(async () => {
  try {
    await resolveJDBCDriver();
  } catch (e: any) {
    await displayCLIError(e, 'Cube.js Arrow Flight SQL JDBC Installer');
  }
})();
