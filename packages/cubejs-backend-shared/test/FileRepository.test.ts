import fs from 'fs-extra';
import os from 'os';
import path from 'path';

import { FileRepository } from '../src/FileRepository';

describe('FileRepository', () => {
  let repositoryPath: string;

  beforeEach(() => {
    repositoryPath = fs.mkdtempSync(path.join(os.tmpdir(), 'cube-models-'));
  });

  afterEach(() => {
    fs.removeSync(repositoryPath);
  });

  test('reads data models from an absolute repository path', async () => {
    fs.writeFileSync(path.join(repositoryPath, 'orders.yml'), 'cubes: []');

    const repository = new FileRepository(repositoryPath);

    expect(repository.localPath()).toBe(repositoryPath);
    await expect(repository.dataSchemaFiles()).resolves.toEqual([
      {
        fileName: 'orders.yml',
        content: 'cubes: []',
      },
    ]);
  });
});
