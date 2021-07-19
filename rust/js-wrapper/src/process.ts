import * as fs from 'fs';
import { ChildProcess, spawn } from 'child_process';
import { withTimeout } from '@cubejs-backend/shared';

import { downloadBinaryFromRelease, getBinaryPath } from './download';

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
    CUBESTORE_PORT: '13306', // TODO MySQL port. Remove it when it becomes optional.
    CUBESTORE_SELECT_WORKERS: '0',
  };

  if (process.env.CUBEJS_LOG_LEVEL === 'trace') {
    env.RUST_BACKTRACE = '1';
  }

  const cubeStore = spawn(pathToExecutable, {
    env,
    detached: false,
  });

  return new Promise<ChildProcess>((resolve, reject) => {
    /**
     * Default Continue Wait timeout in Orchestrator API is 5 seconds, It's not possible to use 15 seconds
     * because pooling will fire it again and again
     */
    const timeout = 4 * 1000;

    let startupExitListener: ((code: number | null) => void) | null = null;

    const startupTimeout = withTimeout(() => {
      if (startupExitListener) {
        cubeStore.off('exit', startupExitListener);
      }

      // Let's kill it, because it's not able to start
      cubeStore.kill();

      reject(
        new Error(
          `Unable to start Cube Store, timeout after ${timeout / 1000}s`
        )
      );
    }, timeout);

    startupExitListener = (code: number | null) => {
      startupTimeout.cancel();

      reject(
        new Error(
          `Cube Store exited with ${code} on startup.`
        )
      );
    };

    cubeStore.on('exit', startupExitListener);

    const processExitListener = () => {
      process.off('exit', processExitListener);

      cubeStore.kill();
    };

    // Just a workaround for https://github.com/nodejs/node/issues/13538
    // It's ok because we dont use CubeStoreHandler.release with force = true
    process.on('exit', processExitListener);

    const startResolver = (data: Buffer) => {
      if (data.toString().includes('MySQL port open on')) {
        // Clear startup timeout killer
        startupTimeout.cancel();
        // Disable start listener, because we resolve Promise
        cubeStore.stdout.off('data', startResolver);

        // Clear startup exit code listener
        if (startupExitListener) {
          cubeStore.off('exit', startupExitListener);
        }

        // Restart can be done, if Cube Store started. Without it we change state to null status and wait next query.
        cubeStore.on('exit', (code) => {
          process.off('exit', processExitListener);

          config.onExit(code);
        });
        resolve(cubeStore);
      }
    };
    cubeStore.stdout.on('data', startResolver);

    cubeStore.stdout.on('data', config.stdout);
    cubeStore.stderr.on('data', config.stderr);
  });
}

export class CubeStoreHandler {
  protected cubeStore: ChildProcess | null = null;

  // Promise that works as mutex, but can be rejected
  protected cubeStoreStarting: Promise<ChildProcess> | null = null;

  // Flag when release was requested, in this state, we skip restart on exit
  protected releaseRequested: boolean = false;

  public constructor(
    protected readonly config: Readonly<CubeStoreHandlerOptions>
  ) {}

  protected async getBinary() {
    const pathToExecutable = getBinaryPath();

    if (!fs.existsSync(pathToExecutable)) {
      await downloadBinaryFromRelease();

      if (!fs.existsSync(pathToExecutable)) {
        throw new Error('Something wrong with downloading Cube Store before running it.');
      }
    }

    return pathToExecutable;
  }

  public async acquire() {
    if (this.cubeStore) {
      return this.cubeStore;
    }

    if (this.cubeStoreStarting) {
      return this.cubeStoreStarting;
    }

    const onExit = (code: number|null) => {
      if (this.releaseRequested) {
        return;
      }

      this.config.onRestart(code);

      this.cubeStoreStarting = new Promise<ChildProcess>(
        (resolve, reject) => startProcess(getBinaryPath(), {
          ...this.config,
          onExit,
        }).then((cubeStore) => {
          this.cubeStore = cubeStore;
          this.cubeStoreStarting = null;

          resolve(cubeStore);
        }).catch((err) => {
          this.cubeStore = null;
          this.cubeStoreStarting = null;

          reject(err);
        })
      );
    };

    this.cubeStoreStarting = new Promise<ChildProcess>((resolve, reject) => this.getBinary()
      .then((pathToExecutable) => {
        startProcess(pathToExecutable, {
          ...this.config,
          onExit,
        }).then((cubeStore) => {
          this.cubeStore = cubeStore;
          this.cubeStoreStarting = null;

          resolve(cubeStore);
        }).catch((err) => {
          this.cubeStore = null;
          this.cubeStoreStarting = null;

          reject(err);
        });
      })
      .catch((err) => {
        this.cubeStore = null;
        this.cubeStoreStarting = null;

        reject(err);
      }));

    return this.cubeStoreStarting;
  }

  public async release(force: boolean = false) {
    // Force, is a compatibility flag, for now we release only in tests
    if (force) {
      if (this.cubeStoreStarting) {
        throw new Error('Something wrong with logic, release was called, while cubestore is starting...');
      }

      this.releaseRequested = true;

      if (this.cubeStore) {
        this.cubeStore.kill('SIGTERM');
      }
    }
  }
}
