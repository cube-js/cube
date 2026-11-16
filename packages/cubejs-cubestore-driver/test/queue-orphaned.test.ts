import { QueuePriority, QueueDriverOptions } from '@cubejs-backend/base-driver';

import { CubeStoreDriver } from '../src/CubeStoreDriver';
import { CubestoreQueueDriverConnection } from '../src/CubeStoreQueueDriver';

jest.mock('@cubejs-backend/native', () => ({
  parseCubestoreResultMessage: jest.fn(),
}));

class RecordingCubeStoreDriver extends CubeStoreDriver {
  public readonly queries: { query: string, values: unknown[] }[] = [];

  public constructor(protected readonly supported: string[]) {
    super();
  }

  public async hasCapability(capability: Parameters<CubeStoreDriver['hasCapability']>[0]): Promise<boolean> {
    return this.supported.includes(capability);
  }

  public async query<R = any>(query: string, values: unknown[]): Promise<R[]> {
    this.queries.push({ query, values });

    return [{ added: 'true', id: '1', pending: '0' }] as R[];
  }
}

const options: QueueDriverOptions = {
  redisQueuePrefix: 'prefix',
  concurrency: 1,
  continueWaitTimeout: 5,
  orphanedTimeout: 120,
  heartBeatTimeout: 120,
};

const addToQueue = async (capabilities: string[], orphanedTimeout?: number) => {
  const driver = new RecordingCubeStoreDriver(capabilities);
  const connection = new CubestoreQueueDriverConnection(driver, options);

  await connection.addToQueue(
    ['SELECT 1', []],
    'query',
    { isJob: false },
    QueuePriority.Interactive,
    { queueId: 1, stageQueryKey: 'stage', requestId: 'request', orphanedTimeout },
  );

  return driver.queries[0];
};

describe('CubestoreQueueDriverConnection.addToQueue ORPHANED', () => {
  it('sends the default orphaned timeout when Cube Store touches on add', async () => {
    const { query, values } = await addToQueue(['queueTouch']);

    expect(query).toContain(' ORPHANED ?');
    expect(values).toContain(options.orphanedTimeout);
  });

  // An older Cube Store gives an item with ORPHANED a shorter expiry and never touches it
  it('leaves it out for an older Cube Store', async () => {
    const { query, values } = await addToQueue([]);

    expect(query).not.toContain('ORPHANED');
    expect(values).not.toContain(options.orphanedTimeout);
  });

  it('sends a query\'s own orphaned timeout whatever the version', async () => {
    const { query, values } = await addToQueue([], 3600);

    expect(query).toContain(' ORPHANED ?');
    expect(values).toContain(3600);
  });
});
