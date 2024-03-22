import { CubeStoreDriver } from '@cubejs-backend/cubestore-driver';
import { QueryQueueBenchmark } from './QueueBench.abstract';

// eslint-disable-next-line import/no-extraneous-dependencies
import 'source-map-support/register';

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

QueryQueueBenchmark(
  'CubeStore Queue',
  {
    cacheAndQueueDriver: 'cubestore',
    cubeStoreDriverFactory,
    beforeAll,
    afterAll
  }
);
