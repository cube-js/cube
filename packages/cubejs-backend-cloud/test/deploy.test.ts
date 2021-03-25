import fs from 'fs-extra';
import path from 'path';

import { DeployDirectory } from '../src/deploy';

const directory = path.join(__dirname, '.deploy');

beforeAll(async () => {
  await fs.mkdir(directory);
  await fs.writeFile(path.join(directory, 'test'), 'some data');
  await fs.writeFile(path.join(directory, '.testdot'), 'some data');
});

afterAll(async () => {
  await fs.remove(directory);
});

test('constuctor', async () => {
  const deployDir = new DeployDirectory({ directory });
  expect(deployDir).not.toBeUndefined();
});

test('fileHashes', async () => {
  const deployDir = new DeployDirectory({ directory });
  const fileHashes = await deployDir.fileHashes();

  expect(fileHashes).toEqual({ test: { hash: 'baf34551fecb48acc3da868eb85e1b6dac9de356' } });
});
