import os from 'os';
import { spawn } from 'child_process';
import fs from 'fs-extra';

export const executeCommand = (command: string, args: string[]) => {
  const child = spawn(command, args, { stdio: 'inherit' });
  return new Promise((resolve, reject) => {
    child.on('close', code => {
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

export const npmInstall = (dependencies: string[], isDev: boolean = false) => executeCommand(
  'npm', ['install', isDev ? '--save-dev' : '--save'].concat(dependencies)
);
