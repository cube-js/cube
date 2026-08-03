import { CubeStoreDevDriver, CubeStoreDriver, CubeStoreHandler } from '@cubejs-backend/cubestore-driver';
import { QueryCacheTest } from '../../unit/QueryCache.abstract';

let beforeAll;
let cubeStoreDriver;
let afterAll = async () => {
  if (cubeStoreDriver) {
    await cubeStoreDriver.release();
  }
};
let cubeStoreDriverFactory = async () => {
  if (cubeStoreDriver) {
    return cubeStoreDriver;
  }

  // eslint-disable-next-line no-return-assign
  return cubeStoreDriver = new CubeStoreDriver({});
};

if ((process.env.CUBEJS_TESTING_CUBESTORE_AUTO_PROVISIONING || 'false') === 'true') {
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
  cubeStoreDriverFactory = async () => new CubeStoreDevDriver(cubeStoreHandler);
}

QueryCacheTest(
  'CubeStore Cache Driver',
  {
    cacheAndQueueDriver: 'cubestore',
    cubeStoreDriverFactory,
    beforeAll,
    afterAll
  }
);
