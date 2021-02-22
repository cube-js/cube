import * as fs from 'fs';
import * as path from 'path';
import { downloadBinaryFromRelease } from './download';
import { spawn } from 'child_process';

const binaryName = process.platform === 'win32' ? 'cubestored.exe' : 'cubestored';

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
