import fs from 'fs-extra';
import path from 'path';
import jwt from 'jsonwebtoken';
import { Config } from '../src/config';

const directory = path.join(__dirname, '.config');

beforeAll(async () => {
  await fs.mkdir(directory);
});

beforeEach(async () => {
  // await fs.writeFile(path.join(directory, '.cubecloud'), JSON.stringify({}));
  await fs.writeFile(path.join(directory, 'config.json'), JSON.stringify({}));
  await fs.writeFile(path.join(directory, '.env'), 'SOME_ENV_NAME=value');
});

afterAll(async () => {
  await fs.remove(directory);
});

test('constuctor', async () => {
  const config = new Config({ directory });
  expect(config).not.toBeUndefined();
});

test('envFile', async () => {
  const config = new Config({ directory });
  const envFile = await config.envFile(path.join(directory, '.env'));
  expect(envFile).toEqual({ SOME_ENV_NAME: 'value' });
});

test('addAuthToken', async () => {
  const config = new Config({ directory });
  const auth = jwt.sign({ url: 'http://localhost:4200', deploymentId: '1' }, 'secret');
  const data = await config.addAuthToken(auth);
  expect(data.auth['http://localhost:4200']).toEqual({ auth });
});
