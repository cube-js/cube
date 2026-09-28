// eslint-disable-next-line import/no-extraneous-dependencies
import 'source-map-support/register';

import { CubeStoreDriver } from '@cubejs-backend/cubestore-driver';
import { QueryQueueBenchmark } from './QueueBench.abstract';

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

const workers = parseInt(process.env.WORKERS || '2', 10);

QueryQueueBenchmark(
  `CubeStore Queue (workers: ${workers})`,
  {
    cacheAndQueueDriver: 'cubestore',
    cubeStoreDriverFactory,
    beforeAll,
    afterAll,
    workers,
  }
);
