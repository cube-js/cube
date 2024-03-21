import { QueryQueueBenchmark } from './Queue.abstract';

const afterAll = async () => {
};

const beforeAll = async () => {
};

QueryQueueBenchmark(
  'Memory Queue',
  {
    cacheAndQueueDriver: 'memory',
    beforeAll,
    afterAll
  }
);
