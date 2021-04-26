import fs from 'fs';
import { pausePromise } from '@cubejs-backend/shared';

import { CubeStoreHandler } from './process';
import { getBinaryPath } from './download';

describe('CubeStoreHandler', () => {
  it('acquire with release', async () => {
    jest.setTimeout(60 * 1000);

    try {
      fs.unlinkSync(getBinaryPath());
    } catch (e) {
      console.log(e);
    }

    const handler = new CubeStoreHandler({
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

    await handler.acquire();

    // It's enough, just to test that it starts.
    await pausePromise(5 * 1000);

    await handler.release(true);
  });
});
