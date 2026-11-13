import { BaseDriver } from '@cubejs-backend/query-orchestrator';

import { OrchestratorDrivers } from '../../src/core/OrchestratorDrivers';
import { SwappedDriverReleases } from '../../src/core/SwappedDriverReleases';
import type { RequestContext } from '../../src/core/types';

class FakeDriver extends BaseDriver {
  public async testConnection(): Promise<void> {
    return undefined;
  }

  public async query<R = unknown>(): Promise<R[]> {
    return [];
  }

  public async release(): Promise<void> {
    return undefined;
  }
}

/** Shows how many failed swaps it remembers. */
class FailingDrivers extends OrchestratorDrivers {
  public get failuresKept(): number {
    return this.failures.size;
  }
}

const contextFor = (token: string): RequestContext => <RequestContext>{
  requestId: `request-${token}`,
  securityContext: { token },
};

describe('OrchestratorDrivers', () => {
  const installed: Array<[string, string | undefined]> = [
    ['no version is installed', undefined],
    ['a version is installed', 'ok'],
  ];

  test.each(installed)('remembers a bounded number of failed swaps while %s', async (_name, version) => {
    const logger = jest.fn();
    const drivers = new FailingDrivers({
      orchestratorId: 'ORCHESTRATOR_ID',
      context: contextFor('probe'),
      version,
      orchestratorOptions: {},
      isCustomDriverFactory: true,
      buildDriver: async (context) => {
        const { token } = context.securityContext;
        if (token !== 'probe') {
          throw new Error(`bad credential ${token}`);
        }

        return new FakeDriver({});
      },
      releases: new SwappedDriverReleases(logger),
      logger,
    });

    await drivers.getDriver('default');

    for (let i = 0; i < 40; i++) {
      const served = drivers.ensureVersion(`v${i}`, contextFor(`v${i}`), () => true);

      if (version === undefined) {
        await expect(served).rejects.toThrow(`bad credential v${i}`);
      } else {
        await expect(served).resolves.toBe(true);
      }

      await drivers.settled();
    }

    expect(drivers.failuresKept).toEqual(16);
  });
});
