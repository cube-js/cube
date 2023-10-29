import logUpdate from 'log-update';
import os from 'os';
import { spawn } from 'cross-spawn';
import fs from 'fs-extra';
import chalk from 'chalk';
import { track, BaseEvent, internalExceptions } from '@cubejs-backend/shared';
import { compare as semverCompare, parse as semverParse, SemVer } from 'semver';

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
  EOL: os.EOL,
});

export const npmInstall = (dependencies: string[], isDev?: boolean) => executeCommand('npm', ['install', isDev ? '--save-dev' : '--save'].concat(dependencies));

export const displayWarning = (message: string) => {
  console.log(`${chalk.yellow('Warning.')} ${message}`);
};

export function loadCliManifest() {
  // eslint-disable-next-line global-require
  return require('../../package.json');
}

export async function event(opts: BaseEvent) {
  try {
    await track({
      ...opts,
      cliVersion: loadCliManifest().version,
    });
  } catch (e: any) {
    internalExceptions(e);
  }
}

export const displayError = async (text: string | string[], options = {}) => {
  console.error('');
  console.error(chalk.cyan('Cube Error ---------------------------------------'));
  console.error('');

  if (Array.isArray(text)) {
    text.forEach((str) => console.error(str));
  } else {
    console.error(text);
  }

  console.error('');
  console.error(chalk.yellow('Need some help? -------------------------------------'));

  await event({
    event: 'Error',
    error: Array.isArray(text) ? text.join('\n') : text.toString(),
    ...options,
  });

  console.error('');
  console.error(`${chalk.yellow('  Ask this question in Cube Slack:')} https://slack.cube.dev`);
  console.error(`${chalk.yellow('  Post an issue:')} https://github.com/cube-js/cube.js/issues`);
  console.error('');

  process.exit(1);
};

export const logStage = async (stage: string, eventName: string, props?: Record<string, any>) => {
  console.log(`- ${stage}`);

  if (eventName) {
    await track({
      event: eventName,
      ...props,
    });
  }
};

export function findMaxVersion(versions: string[]): SemVer {
  return versions.map((v) => <SemVer>semverParse(v)).reduce((a, b) => (semverCompare(a, b) === 1 ? a : b));
}

export function debounce<T extends(...args: any[]) => void>(func: T, delay: number): (...args: Parameters<T>) => void {
  let timeoutId: ReturnType<typeof setTimeout> | undefined;
  return (...args: Parameters<T>): void => {
    if (timeoutId) {
      clearTimeout(timeoutId);
    }
    timeoutId = setTimeout(() => {
      func(...args);
    }, delay);
  };
}

export function createLogger() {
  let index = 0;
  const frames = ['-', '\\', '|', '/'];
  let spinnerIntervalId;

  return {
    spin(text: string) {
      spinnerIntervalId = setInterval(() => {
        const frame = frames[(index = ++index % frames.length)];
        logUpdate(`${frame} ${text}`);
      }, 80);
    },
    ready(text: string) {
      clearInterval(spinnerIntervalId);
      logUpdate(`✔ ${text}`);
    },
    persist() {
      clearInterval(spinnerIntervalId);
      logUpdate.done();
    },
    clear() {
      clearInterval(spinnerIntervalId);
      logUpdate.clear();
    },
  };
}
