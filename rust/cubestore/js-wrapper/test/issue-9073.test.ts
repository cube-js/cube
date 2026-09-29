import fs from 'fs';
import os from 'os';
import path from 'path';
import { ChildProcess } from 'child_process';

import { CubeStoreHandler } from '../src/process';

// https://github.com/cube-js/cube/issues/9073
// Cube Store (dev mode, spawned by CubeStoreHandler) fails to start with
// CUBESTORE_LOG_LEVEL=error or warn, because startup readiness is detected by
// waiting for the INFO-level "MySQL port open on" log line on stdout.
class CubeStoreHandlerOpen extends CubeStoreHandler {
  public cubeStore: ChildProcess | null = null;

  protected async getBinary() {
    // Allow using an already downloaded binary to avoid re-downloading it
    if (process.env.CUBESTORE_TEST_BINARY) {
      return process.env.CUBESTORE_TEST_BINARY;
    }

    return super.getBinary();
  }
}

describe('Issue #9073: CUBESTORE_LOG_LEVEL', () => {
  jest.setTimeout(60 * 1000);

  const envBackup = { ...process.env };

  afterEach(() => {
    process.env = { ...envBackup };
  });

  for (const level of ['error', 'warn', 'info', 'trace']) {
    it(`starts with CUBESTORE_LOG_LEVEL=${level}`, async () => {
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), `cubestore-9073-${level}-`));

      process.env.CUBESTORE_LOG_LEVEL = level;
      process.env.CUBESTORE_DATA_DIR = path.join(dir, 'data');
      process.env.CUBESTORE_REMOTE_DIR = path.join(dir, 'remote');
      process.env.CUBESTORE_HTTP_PORT = process.env.CUBESTORE_TEST_HTTP_PORT || '3131';
      process.env.CUBESTORE_STATUS_PORT = process.env.CUBESTORE_TEST_STATUS_PORT || '3132';
      process.env.CUBESTORE_TELEMETRY = 'false';

      const handler = new CubeStoreHandlerOpen({
        stdout: (v) => {
          console.log(v.toString());
        },
        stderr: (v) => {
          console.log(v.toString());
        },
        onRestart: () => {
          throw new Error('Process should not restart, while we are testing it!');
        },
      });

      try {
        await expect(handler.acquire()).resolves.toBeDefined();
      } finally {
        await handler.release(true);
        // Give the process time to free ports before the next case
        await new Promise((resolve) => setTimeout(resolve, 2000));
        fs.rmSync(dir, { recursive: true, force: true });
      }
    });
  }
});
