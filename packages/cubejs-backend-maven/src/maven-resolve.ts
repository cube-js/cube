import 'source-map-support/register';

import { displayCLIError } from '@cubejs-backend/shared';
import { resolveDependencies, getDependenciesFromPackage } from './maven';

(async () => {
  try {
    await resolveDependencies(getDependenciesFromPackage(), {
      showOutput: true,
    });
  } catch (e: any) {
    await displayCLIError(e, 'Maven Installer');
  }
})();
