import { CubeStoreDriver } from '@cubejs-backend/cubestore-driver';
import { QueryQueueTest } from '../../unit/QueryQueue.abstract';

let cubeStoreDriver;
const afterAll = async () => {
  if (cubeStoreDriver) {
    await cubeStoreDriver.release();
  }
};
const cubeStoreDriverFactory = async () => {
  if (cubeStoreDriver) {
    return cubeStoreDriver;
  }

  // eslint-disable-next-line no-return-assign
  return cubeStoreDriver = new CubeStoreDriver({});
};
const beforeAll = async () => {
  await (await cubeStoreDriverFactory()).query('QUEUE TRUNCATE');
};

QueryQueueTest(
  'CubeStore Queue Driver',
  {
    cacheAndQueueDriver: 'cubestore',
    cubeStoreDriverFactory,
    beforeAll,
    afterAll
  }
);
