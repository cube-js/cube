import * as fs from 'fs';
import * as path from 'path';
import { ChildProcess, spawn } from 'child_process';
import { pausePromise } from '@cubejs-backend/shared';

import { downloadBinaryFromRelease } from './download';

const binaryName = process.platform === 'win32' ? 'cubestored.exe' : 'cubestored';

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

export function isCubeStoreSupported(): boolean {
  if (process.arch !== 'x64') {
    return false;
  }

  return ['win32', 'darwin', 'linux'].includes(process.platform);
}

export class CubeStoreHandler {
  protected cubeStore: Promise<ChildProcess> | null = null;

  public constructor(
    protected readonly config: Readonly<CubeStoreHandlerOptions>
  ) {}

  public async acquire() {
    if (this.cubeStore) {
      return this.cubeStore;
    }

    // eslint-disable-next-line no-async-promise-executor
    this.cubeStore = new Promise<ChildProcess>(async (resolve) => {
      const pathToExecutable = path.join(__dirname, '..', 'downloaded', 'latest', 'bin', binaryName);

      if (!fs.existsSync(pathToExecutable)) {
        await downloadBinaryFromRelease();

        if (!fs.existsSync(pathToExecutable)) {
          throw new Error('Something wrong with downloading Cube Store before running it.');
        }
      }

      const onExit = (code: number|null) => {
        this.config.onRestart(code);

        this.cubeStore = startProcess(pathToExecutable, {
          ...this.config,
          onExit
        });
      };

      this.cubeStore = startProcess(pathToExecutable, {
        ...this.config,
        onExit
      });

      resolve(this.cubeStore);
    });

    return this.cubeStore;
  }

  public async release() {
    // @todo Use SIGTERM for gracefully shutdown?
  }
}
