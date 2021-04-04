import fs from 'fs-extra';
import path from 'path';
import jwt from 'jsonwebtoken';

import { LivePreviewWatcher } from '../src/live-preview';

const directory = path.join(__dirname, '.livepreview');

beforeAll(async () => {
  await fs.mkdir(directory);
});

beforeEach(async () => {
  await fs.writeFile(path.join(directory, 'index.js'), JSON.stringify({}));
});

afterAll(async () => {
  await fs.remove(directory);
});

test('LivePreviewWatcher: constuctor', async () => {
  const livePreviewWatcher = new LivePreviewWatcher();
  expect(livePreviewWatcher).not.toBeUndefined();
});

test('setAuth', async () => {
  const livePreviewWatcher = new LivePreviewWatcher();
  const payload = {
    userId: 1,
    url: 'http://localhost:4200'
  };
  const deploymentId = '1';

  const token = jwt.sign(payload, 'secret');
  const auth = livePreviewWatcher.setAuth(token, deploymentId);
  expect(auth).toEqual({
    auth: token,
    url: payload.url,
    deploymentId
  });
});
