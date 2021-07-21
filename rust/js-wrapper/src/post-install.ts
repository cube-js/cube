import 'source-map-support/register';

import * as process from 'process';
import { displayCLIError, displayCLIWarning } from '@cubejs-backend/shared';

import { downloadBinaryFromRelease } from './download';
import { isCubeStoreSupported } from './utils';

(async () => {
  try {
    if (isCubeStoreSupported()) {
      await downloadBinaryFromRelease();
    } else {
      displayCLIWarning(
        `You are using ${process.platform} platform with ${process.arch} architecture, ` +
        'which is not supported by Cube Store. Installation will be skipped.'
      );
    }
  } catch (e) {
    await displayCLIError(e, 'Cube Store Installer');
  }
})();
