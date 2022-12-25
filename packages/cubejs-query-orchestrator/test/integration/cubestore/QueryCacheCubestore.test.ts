import { CubeStoreDevDriver, CubeStoreDriver, CubeStoreHandler } from '@cubejs-backend/cubestore-driver';
import { QueryCacheTest } from '../../unit/QueryCache.abstract';

let beforeAll;
let afterAll;
let cubeStoreDriver = new CubeStoreDriver({});

if ((process.env.CUBEJS_TESTING_CUBESTORE_AUTO_PROVISIONING || 'true') === 'true') {
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

  beforeAll = async () => {
    await cubeStoreHandler.acquire();
  };
  afterAll = async () => cubeStoreHandler.release(true);
  cubeStoreDriver = new CubeStoreDevDriver(cubeStoreHandler);
}

QueryCacheTest(
  'CubeStore Cache Driver',
  {
    cacheAndQueueDriver: 'cubestore',
    cubeStoreDriver,
    beforeAll,
    afterAll
  }
);
