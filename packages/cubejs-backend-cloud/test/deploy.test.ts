import fs from 'fs-extra';
import path from 'path';

import { DeployDirectory, DeployController } from '../src/deploy';
import { CubeCloudClient } from '../src/cloud';

const directory = path.join(__dirname, '.deploy');

beforeAll(async () => {
  await fs.mkdir(directory);
  await fs.writeFile(path.join(directory, 'test'), 'some data');
  await fs.writeFile(path.join(directory, '.testdot'), 'some data');
});

afterAll(async () => {
  await fs.remove(directory);
});

test('DeployDirectory: constuctor', async () => {
  const deployDir = new DeployDirectory({ directory });
  expect(deployDir).not.toBeUndefined();
});

test('DeployDirectory: fileHashes', async () => {
  const deployDir = new DeployDirectory({ directory });
  const fileHashes = await deployDir.fileHashes();

  expect(fileHashes).toEqual({ test: { hash: 'baf34551fecb48acc3da868eb85e1b6dac9de356' } });
});

test('DeployController: constuctor', async () => {
  const deployDir = new DeployController(new CubeCloudClient());
  expect(deployDir).not.toBeUndefined();
});
