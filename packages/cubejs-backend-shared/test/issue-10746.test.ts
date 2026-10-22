// https://github.com/cube-js/cube/issues/10746
// An absolute CUBEJS_SCHEMA_PATH / schemaPath must be honoured instead of being
// joined onto process.cwd() (which silently loads zero model files).
import os from 'os';
import path from 'path';
import fs from 'fs-extra';

import { FileRepository } from '../src';

describe('FileRepository with absolute repository path (#10746)', () => {
  let modelDir: string;
  let cwdDir: string;
  let originalCwd: string;

  beforeEach(async () => {
    originalCwd = process.cwd();
    modelDir = await fs.mkdtemp(path.join(os.tmpdir(), 'cube-10746-model-'));
    cwdDir = await fs.mkdtemp(path.join(os.tmpdir(), 'cube-10746-cwd-'));
    await fs.writeFile(
      path.join(modelDir, 'orders.yml'),
      'cubes:\n  - name: orders\n    sql: "SELECT 1 AS id"\n'
    );
    process.chdir(cwdDir);
  });

  afterEach(async () => {
    process.chdir(originalCwd);
    await fs.remove(modelDir);
    await fs.remove(cwdDir);
  });

  test('localPath() returns the absolute path as-is', () => {
    const repo = new FileRepository(modelDir);
    expect(repo.localPath()).toBe(modelDir);
  });

  test('dataSchemaFiles() reads files from the absolute path', async () => {
    const repo = new FileRepository(modelDir);
    const files = await repo.dataSchemaFiles();
    expect(files.map(f => f.fileName)).toEqual(['orders.yml']);
  });

  test('relative path still resolves against process.cwd()', async () => {
    const repo = new FileRepository(path.relative(cwdDir, modelDir));
    const files = await repo.dataSchemaFiles();
    expect(files.map(f => f.fileName)).toEqual(['orders.yml']);
  });
});
