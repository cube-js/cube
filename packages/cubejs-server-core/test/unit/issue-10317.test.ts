import { CubejsServerCore } from '../../src';

// https://github.com/cube-js/cube/issues/10317
// `external_driver_factory` in cube.py can only return a dict (Python cannot build a JS
// driver instance), and the Python config bridge hands that dict to server-core as a
// plain object. `driverFactory` converts such a config via `resolveDriver()`, but the
// `externalDriverFactory` wrapper in `CubejsServerCore.getOrchestratorApi` uses the value
// as a driver as-is, failing with `driver.release is not a function`.
//
// The Python bridge itself is not exercised here (the native module in this environment
// is a fallback build without the `python` feature): the factory below returns the same
// plain object the bridge would.
describe('issue #10317: externalDriverFactory returning a driver config', () => {
  let core: CubejsServerCore;

  afterEach(async () => {
    await core?.releaseConnections();
  });

  test('a config object returned by externalDriverFactory is turned into a driver', async () => {
    core = new CubejsServerCore(<any>{
      apiSecret: 'secret',
      devServer: false,
      orchestratorOptions: { rollupOnlyMode: true },
      driverFactory: () => ({ type: 'duckdb' }),
      externalDriverFactory: () => ({ type: 'duckdb' }),
    });

    const api = await core.getOrchestratorApi(<any>{
      requestId: 'issue-10317',
      authInfo: null,
      securityContext: {},
    });

    // In rollup-only mode this tests the external (pre-aggregations) driver only
    await expect(api.testConnection()).resolves.toBeUndefined();
  });
});
