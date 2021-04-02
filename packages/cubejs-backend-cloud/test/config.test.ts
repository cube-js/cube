import fs from 'fs-extra';
import path from 'path';
import jwt from 'jsonwebtoken';
import { Config } from '../src/config';

const directory = path.join(__dirname, '.config');
const home = path.join(__dirname, '.home');

beforeAll(async () => {
  await fs.mkdir(directory);
  await fs.mkdir(home);
});

beforeEach(async () => {
  await fs.writeFile(path.join(directory, 'config.json'), JSON.stringify({}));
  await fs.writeFile(path.join(directory, '.env'), 'SOME_ENV_NAME=value');
});

afterAll(async () => {
  await fs.remove(directory);
  await fs.remove(home);
});

test('Config: constuctor', async () => {
  const config = new Config({ directory, home });
  expect(config).not.toBeUndefined();
});

test('envFile', async () => {
  const config = new Config({ directory, home });
  const envFile = await config.envFile(path.join(directory, '.env'));
  expect(envFile).toEqual({ SOME_ENV_NAME: 'value' });
});

test('addAuthToken', async () => {
  const config = new Config({ directory, home });
  const auth = jwt.sign({ url: 'http://localhost:4200', deploymentId: '1' }, 'secret');
  const data = await config.addAuthToken(auth);
  expect(data.auth['http://localhost:4200']).toEqual({ auth });
});
