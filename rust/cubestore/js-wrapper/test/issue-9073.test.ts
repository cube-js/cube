import fs from 'fs';
import os from 'os';
import path from 'path';

import { CubeStoreHandler } from '../src/process';

// https://github.com/cube-js/cube/issues/9073
// CubeStoreHandler detects a successful start by waiting for the INFO-level
// `MySQL port open on` line on Cube Store's stdout. With CUBESTORE_LOG_LEVEL=error
// (or warn) that line is never logged, so a healthy Cube Store is killed after the
// 4s startup timeout and the dev server fails with "Unable to start Cube Store".
//
// Runs the real `cubestored` binary (downloaded by `acquire()` if missing).
describe('issue #9073: CUBESTORE_LOG_LEVEL=error', () => {
  jest.setTimeout(60 * 1000);

  const envKeys = ['CUBESTORE_LOG_LEVEL', 'CUBESTORE_DATA_DIR', 'CUBESTORE_REMOTE_DIR', 'CUBESTORE_HTTP_PORT'];
  const savedEnv: Record<string, string | undefined> = {};
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'cubestore-issue-9073-'));
    for (const key of envKeys) {
      savedEnv[key] = process.env[key];
    }
    process.env.CUBESTORE_DATA_DIR = path.join(tmpDir, 'data');
    process.env.CUBESTORE_REMOTE_DIR = path.join(tmpDir, 'remote');
    process.env.CUBESTORE_HTTP_PORT = '23030';
  });

  afterEach(() => {
    for (const key of envKeys) {
      if (savedEnv[key] === undefined) {
        delete process.env[key];
      } else {
        process.env[key] = savedEnv[key];
      }
    }
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it.each(['error', 'warn'])('starts with CUBESTORE_LOG_LEVEL=%s', async (level) => {
    process.env.CUBESTORE_LOG_LEVEL = level;

    const handler = new CubeStoreHandler({
      stdout: () => undefined,
      stderr: () => undefined,
      onRestart: () => {
        throw new Error('Cube Store should not restart');
      },
    });

    try {
      await expect(handler.acquire()).resolves.toBeDefined();
    } finally {
      await handler.release(true);
    }
  });
});
