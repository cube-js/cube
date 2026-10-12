import os from 'os';
import path from 'path';
import fs from 'fs-extra';

import { FileRepository } from '../src/FileRepository';

// https://github.com/cube-js/cube/issues/10746
describe('Issue #10746: absolute CUBEJS_SCHEMA_PATH', () => {
  let tmpDir: string;

  beforeEach(async () => {
    tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'cube-issue-10746-'));
    await fs.ensureDir(path.join(tmpDir, 'cubes'));
    await fs.writeFile(
      path.join(tmpDir, 'cubes', 'orders.yml'),
      'cubes:\n  - name: orders\n    sql: "SELECT 1 AS id"\n'
    );
  });

  afterEach(async () => {
    await fs.remove(tmpDir);
    // The buggy path.join() + fs.ensureDir() materialises an empty copy under cwd
    const stray = path.join(process.cwd(), tmpDir);
    await fs.remove(stray);
    const strayParent = path.dirname(stray);
    if ((await fs.pathExists(strayParent)) && (await fs.readdir(strayParent)).length === 0) {
      await fs.rmdir(strayParent);
    }
  });

  it('localPath() honours an absolute repository path', () => {
    const repo = new FileRepository(tmpDir);
    expect(repo.localPath()).toEqual(tmpDir);
  });

  it('loads model files from an absolute repository path', async () => {
    const repo = new FileRepository(tmpDir);
    const files = await repo.dataSchemaFiles();
    expect(files.map(f => f.fileName)).toEqual([path.join('cubes', 'orders.yml')]);
  });
});
