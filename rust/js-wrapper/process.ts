import * as fs from 'fs';
import * as path from 'path';
import { ChildProcess, spawn } from 'child_process';
import { pausePromise } from '@cubejs-backend/shared';

import { downloadBinaryFromRelease } from './download';

const binaryName = process.platform === 'win32' ? 'cubestored.exe' : 'cubestored';

export interface CubeStoreHandler {
  acquire: () => Promise<void>;
  release: () => Promise<void>;
}

export interface CubeStoreHandlerOptions {
  stdout: (data: Buffer) => void;
  stderr: (data: Buffer) => void;
  onRestart: (exitCode: number|null) => void;
}

interface StartProcessOptions extends CubeStoreHandlerOptions {
  onExit: (code: number | null) => void;
}

async function startProcess(pathToExecutable: string, config: Readonly<StartProcessOptions>) {
  const env: Record<string, string> = {
    CUBESTORE_PORT: '13306',
    CUBESTORE_SELECT_WORKERS: '0',
  };

  if (process.env.CUBEJS_LOG_LEVEL === 'trace') {
    env.RUST_BACKTRACE = '1';
  }

  const cubeStore = spawn(pathToExecutable, {
    env,
  });

  cubeStore.on('error', (err) => {
    console.error('Failed to start subprocess.');
    console.error(err);

    process.exit(1);
  });

  cubeStore.on('exit', config.onExit);

  cubeStore.stdout.on('data', config.stdout);
  cubeStore.stderr.on('data', config.stderr);

  // @todo We need to implement better awaiting on startup of the Cube Store
  // Probably, it should be IPC, because parsing stdout on message, is a bad idea
  await pausePromise(500);

  return cubeStore;
}

export async function startCubeStoreHandler(config: Readonly<CubeStoreHandlerOptions>): Promise<CubeStoreHandler> {
  const pathToExecutable = path.join(__dirname, '..', 'downloaded', 'latest', 'bin', binaryName);

  if (!fs.existsSync(pathToExecutable)) {
    await downloadBinaryFromRelease();

    if (!fs.existsSync(pathToExecutable)) {
      throw new Error('Something wrong with downloading');
    }
  }

  let cubeStore: Promise<ChildProcess> | null = null;

  const onExit = (code: number|null) => {
    config.onRestart(code);

    cubeStore = startProcess(pathToExecutable, {
      ...config,
      onExit
    });
  };

  cubeStore = startProcess(pathToExecutable, {
    ...config,
    onExit
  });

  return {
    acquire: async () => {
      if (cubeStore) {
        await cubeStore;
      }
    },
    release: async () => {
      // @todo Use SIGTERM for gracefully shutdown?
      // if (cubeStore) {
      //   (await cubeStore).kill();
      // }
    },
  };
}
