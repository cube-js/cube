import * as fs from 'fs';
import * as path from 'path';
import { spawn } from 'child_process';
import { downloadBinaryFromRelease } from './download';

const binaryName = process.platform === 'win32' ? 'cubestored.exe' : 'cubestored';

// eslint-disable-next-line import/prefer-default-export
export async function startCubeStore() {
  const pathToExecutable = path.join(__dirname, '..', 'bin', binaryName);

  if (!fs.existsSync(pathToExecutable)) {
    await downloadBinaryFromRelease();

    if (!fs.existsSync(pathToExecutable)) {
      throw new Error('Something wrong with downloading');
    }
  }

  return spawn(pathToExecutable, {
    env: {
      CUBESTORE_PORT: '13306',
    }
  });
}
