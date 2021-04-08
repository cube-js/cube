import 'source-map-support/register';

import { displayCLIError } from '@cubejs-backend/shared';
import { resolveDependencies, getDependenciesFromPackage } from './maven';

(async () => {
  try {
    await resolveDependencies(getDependenciesFromPackage(), {
      showOutput: true,
    });
  } catch (e) {
    await displayCLIError(e, 'Maven Installer');
  }
})();
