// Cube Store lists cache keys by the part before their last `:`, which the local cache driver
// doesn't, so orchestrators that share a pre-aggregation id are checked against Cube Store here.

import { CubeStoreDevDriver, CubeStoreDriver, CubeStoreHandler } from '@cubejs-backend/cubestore-driver';
import { testLogger } from '@cubejs-backend/shared';
import { QueryOrchestrator } from '../../../src/orchestrator/QueryOrchestrator';

let cubeStoreDriver: CubeStoreDriver | undefined;
let cubeStoreHandler: CubeStoreHandler | undefined;

const cubeStoreDriverFactory = async (): Promise<CubeStoreDriver> => {
  if (!cubeStoreDriver) {
    cubeStoreDriver = cubeStoreHandler ? new CubeStoreDevDriver(cubeStoreHandler) : new CubeStoreDriver({});
  }

  return cubeStoreDriver;
};

if ((process.env.CUBEJS_TESTING_CUBESTORE_AUTO_PROVISIONING || 'false') === 'true') {
  cubeStoreHandler = new CubeStoreHandler({
    stdout: (data) => console.log(data.toString().trim()),
    stderr: (data) => console.log(data.toString().trim()),
    onRestart: (code) => console.log({ warning: `Instance exit with ${code}, restarting` }),
  });
}

describe('Shared pre-aggregation orchestrator id on Cube Store', () => {
  jest.setTimeout(60 * 1000);

  const orchestrators: QueryOrchestrator[] = [];

  const createOrchestrator = (orchestratorId: string, preAggregationsRedisPrefix: string) => {
    const orchestrator = new QueryOrchestrator(
      orchestratorId,
      async () => {
        throw new Error('No source database in this test');
      },
      testLogger,
      {
        cacheAndQueueDriver: 'cubestore',
        externalDriverFactory: cubeStoreDriverFactory,
        preAggregationsRedisPrefix,
        preAggregationsOptions: {
          touchTablePersistTime: 3600,
        },
      },
    );
    orchestrators.push(orchestrator);
    return orchestrator;
  };

  beforeAll(async () => {
    if (cubeStoreHandler) {
      await cubeStoreHandler.acquire();
    }
  });

  afterAll(async () => {
    if (cubeStoreDriver) {
      await cubeStoreDriver.release();
    }

    if (cubeStoreHandler) {
      await cubeStoreHandler.release(true);
    }
  });

  test('drop without touch waits for an orchestrator whose id has a colon', async () => {
    const testId = Date.now();
    const sharedId = `SHARED_PRE_AGG_${testId}`;
    const appA = createOrchestrator(`APP_A_${testId}`, sharedId);
    const appB = createOrchestrator(`tenant:v2_${testId}`, sharedId);

    await appB.markRefreshActive();
    await appA.updateRefreshEndReached();

    // App B refreshes but hasn't finished, so its tables may not be touched yet
    expect(await appA.getPreAggregations().getRefreshEndReached()).toBeFalsy();

    await appB.updateRefreshEndReached();

    expect(await appA.getPreAggregations().getRefreshEndReached()).toBeTruthy();
  });
});
