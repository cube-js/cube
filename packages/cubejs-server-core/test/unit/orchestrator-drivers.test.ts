import { BaseDriver } from '@cubejs-backend/query-orchestrator';

import { OrchestratorDrivers, SwapRun } from '../../src/core/OrchestratorDrivers';
import { SwappedDriverReleases } from '../../src/core/SwappedDriverReleases';
import type { RequestContext } from '../../src/core/types';

class FakeDriver extends BaseDriver {
  public constructor(public readonly token: string) {
    super({});
  }

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

/** Asks for a driver never built before right after the first rebuild round, before the install. */
class LateLookupDrivers extends OrchestratorDrivers {
  public lateLookup: Promise<BaseDriver> | undefined;

  protected async rebuildRound(run: SwapRun, pending: Map<Promise<BaseDriver>, string>) {
    await super.rebuildRound(run, pending);

    if (!this.lateLookup) {
      this.lateLookup = this.getDriver('late');
    }
  }
}

/** Shows how many swap errors it keeps. */
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
  test('a driver first asked for between the rebuild and the install is rebuilt from the new context', async () => {
    const logger = jest.fn();
    const drivers = new LateLookupDrivers({
      orchestratorId: 'ORCHESTRATOR_ID',
      context: contextFor('v1'),
      version: 'v1',
      orchestratorOptions: {},
      isCustomDriverFactory: () => true,
      buildDriver: async (context) => new FakeDriver(context.securityContext.token),
      releases: new SwappedDriverReleases(logger),
      logger,
    });

    await drivers.getDriver('default');
    drivers.ensureVersion('v2', contextFor('v2'), () => true);
    await drivers.settled();

    expect(drivers.lateLookup).toBeDefined();
    expect(<FakeDriver>(await drivers.getDriver('default'))).toMatchObject({ token: 'v2' });
    expect(<FakeDriver>(await drivers.getDriver('late'))).toMatchObject({ token: 'v2' });
  });

  function failingDrivers(version: string | undefined) {
    const logger = jest.fn();

    return new FailingDrivers({
      orchestratorId: 'ORCHESTRATOR_ID',
      context: contextFor('probe'),
      version,
      orchestratorOptions: {},
      isCustomDriverFactory: () => true,
      buildDriver: async (context) => {
        const { token } = context.securityContext;
        if (token !== 'probe' && token !== 'ok') {
          throw new Error(`bad credential ${token}`);
        }

        return new FakeDriver(token);
      },
      releases: new SwappedDriverReleases(logger),
      logger,
    });
  }

  test('keeps a bounded number of swap errors while no version is installed', async () => {
    const drivers = failingDrivers(undefined);
    await drivers.getDriver('default');

    for (let i = 0; i < 40; i++) {
      await expect(drivers.ensureVersion(`v${i}`, contextFor(`v${i}`), () => true)).rejects.toThrow(`bad credential v${i}`);
    }

    expect(drivers.failuresKept).toEqual(16);
  });

  test('keeps no swap errors once a version is installed', async () => {
    const drivers = failingDrivers('ok');
    await drivers.getDriver('default');

    for (let i = 0; i < 5; i++) {
      await drivers.ensureVersion(`v${i}`, contextFor(`v${i}`), () => true);
      await drivers.settled();
    }

    expect(drivers.failuresKept).toEqual(0);
  });
});
