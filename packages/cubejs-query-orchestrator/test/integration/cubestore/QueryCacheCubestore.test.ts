import { CubeStoreDevDriver, CubeStoreHandler } from '@cubejs-backend/cubestore-driver';
import { QueryCacheTest } from '../../unit/QueryCache.abstract';

const cubeStoreHandler = new CubeStoreHandler({
  stdout: (data) => {
    console.log(data.toString().trim());
  },
  stderr: (data) => {
    console.log(data.toString().trim());
  },
  onRestart: (code) => console.log({
    warning: `Instance exit with ${code}, restarting`,
  }),
});

QueryCacheTest(
  'CubeStore Cache Driver',
  {
    cacheAndQueueDriver: 'cubestore',
    cubeStoreDriver: new CubeStoreDevDriver(cubeStoreHandler),
    beforeAll: async () => {
      await cubeStoreHandler.acquire();
    },
    afterAll: async () => cubeStoreHandler.release(true)
  }
);
