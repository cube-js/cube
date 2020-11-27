import os from 'os';
import { spawn } from 'cross-spawn';
import fs from 'fs-extra';
import path from 'path';
import chalk from 'chalk';
import { machineIdSync } from 'node-machine-id';

import { track } from './track';

export const isDockerImage = () => Boolean(process.env.CUBEJS_DOCKER_IMAGE_TAG);

export const executeCommand = (command: string, args: string[]) => {
  const child = spawn(command, args, { stdio: 'inherit' });

  return new Promise<void>((resolve, reject) => {
    child.on('close', (code: number) => {
      if (code !== 0) {
        reject(new Error(`${command} ${args.join(' ')} failed with exit code ${code}`));
        return;
      }

      resolve();
    });
  });
};

export const writePackageJson = async (json: any) => fs.writeJson('package.json', json, {
  spaces: 2,
  EOL: os.EOL
});

export const npmInstall = (dependencies: string[], isDev?: boolean) => executeCommand(
  'npm', ['install', isDev ? '--save-dev' : '--save'].concat(dependencies)
);

const anonymousId = machineIdSync();

export const event = async (name: string, props?: any) => {
  try {
    await track({
      event: name,
      anonymousId,
      ...props
    });
  } catch (e) {
    // ignore
  }
};

export const displayError = async (text: string|string[], options = {}) => {
  console.error('');
  console.error(chalk.cyan('Cube.js Error ---------------------------------------'));
  console.error('');

  if (Array.isArray(text)) {
    text.forEach((str) => console.error(str));
  } else {
    console.error(text);
  }

  console.error('');
  console.error(chalk.yellow('Need some help? -------------------------------------'));

  await event('Error', { error: Array.isArray(text) ? text.join('\n') : text.toString(), ...options });

  console.error('');
  console.error(`${chalk.yellow('  Ask this question in Cube.js Slack:')} https://slack.cube.dev`);
  console.error(`${chalk.yellow('  Post an issue:')} https://github.com/cube-js/cube.js/issues`);
  console.error('');

  process.exit(1);
};

export const packageExists = (moduleName: string, relative: boolean = false) => {
  if (relative) {
    try {
      // eslint-disable-next-line global-require,import/no-dynamic-require
      require.resolve(`${moduleName}`);

      return true;
    } catch (error) {
      return false;
    }
  }

  const modulePath = path.join(process.cwd(), 'node_modules', moduleName);
  return fs.pathExistsSync(modulePath);
};

const requiredPackageExists = async (moduleName: string, relative: boolean = false) => {
  if (!packageExists(moduleName, relative)) {
    await displayError(
      `${moduleName} dependency not found. Please run this command from project directory.`
    );
  }
};

export const requireFromPackage = async <T = any>(moduleName: string, relative: boolean = false): Promise<T> => {
  await requiredPackageExists(moduleName, relative);

  if (relative) {
    const resolvePath = require.resolve(`${moduleName}`);

    // eslint-disable-next-line global-require,import/no-dynamic-require
    return require(resolvePath);
  }

  // eslint-disable-next-line global-require,import/no-dynamic-require
  return require(path.join(process.cwd(), 'node_modules', moduleName));
};

export const logStage = async (stage: string, eventName: string, props?: any) => {
  console.log(`- ${stage}`);
  if (eventName) {
    await event(eventName, props);
  }
};

export function loadCliManifest() {
  // eslint-disable-next-line global-require
  return require('../package.json');
}
