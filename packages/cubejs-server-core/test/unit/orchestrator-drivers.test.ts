import { BaseDriver } from '@cubejs-backend/query-orchestrator';

import { OrchestratorDrivers, SwapRun } from '../../src/core/OrchestratorDrivers';
import { SwappedDriverReleases } from '../../src/core/SwappedDriverReleases';
import type { RequestContext } from '../../src/core/types';

class FakeDriver extends BaseDriver {
  public constructor(public readonly token: string) {
    super({});
  }

  public async testConnection() {
    return undefined;
  }

  public async query<R = unknown>(): Promise<R[]> {
    return [];
  }

  public async release() {
    return undefined;
  }
}

/** Asks for a driver never built before right after the rebuild, before the install. */
class LateLookupDrivers extends OrchestratorDrivers {
  public lateLookup: Promise<BaseDriver> | undefined;

  protected async rebuildAll(run: SwapRun) {
    await super.rebuildAll(run);

    if (!this.lateLookup) {
      this.lateLookup = this.getDriver('late');
    }
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
    await drivers.ensureVersion('v2', contextFor('v2'), () => true);

    expect(drivers.lateLookup).toBeDefined();
    expect(<FakeDriver>(await drivers.getDriver('default'))).toMatchObject({ token: 'v2' });
    expect(<FakeDriver>(await drivers.getDriver('late'))).toMatchObject({ token: 'v2' });
  });
});
