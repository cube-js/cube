// eslint-disable-next-line import/no-extraneous-dependencies
import 'source-map-support/register';

import { QueryQueueBenchmark } from './QueueBench.abstract';

const afterAll = async () => {
  // nothing to do
};

const beforeAll = async () => {
  // nothing to do
};

QueryQueueBenchmark(
  'Memory Queue',
  {
    cacheAndQueueDriver: 'memory',
    beforeAll,
    afterAll
  }
);
