/* eslint-disable no-restricted-syntax */
import * as process from 'process';
import color from '@oclif/color';

import { downloadBinaryFromRelease } from './download';
import { displayWarning, isCubeStoreSupported } from './utils';

const displayError = async (text: string) => {
  console.error('');
  console.error(color.cyan('Cube.js CubeStore Installer ---------------------------------------'));
  console.error('');

  console.error(text);

  console.error('');
  console.error(color.yellow('Need some help? -------------------------------------'));

  console.error('');
  console.error(`${color.yellow('  Ask this question in Cube.js Slack:')} https://slack.cube.dev`);
  console.error(`${color.yellow('  Post an issue:')} https://github.com/cube-js/cube.js/issues`);
  console.error('');

  process.exit(1);
};

(async () => {
  try {
    if (isCubeStoreSupported()) {
      await downloadBinaryFromRelease();
    } else {
      displayWarning(
        `You are using ${process.platform} platform with ${process.arch} architecture, ` +
        `which is not supported by Cube Store. Installation will be skipped.`
      );
    }
  } catch (e) {
    await displayError(e);
  }
})();
