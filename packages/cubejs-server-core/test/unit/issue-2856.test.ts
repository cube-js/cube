import { CubejsServerCore } from '../../src';

// https://github.com/cube-js/cube/issues/2856
// CUBEJS_LOG_LEVEL is applied only inside the built-in loggers (createLogger). When a
// custom `logger` is configured it is used verbatim, so it keeps receiving info/trace
// messages such as "Load Request" / "Query started" even with CUBEJS_LOG_LEVEL=error.
describe('issue #2856: CUBEJS_LOG_LEVEL with a custom logger', () => {
  const prevLevel = process.env.CUBEJS_LOG_LEVEL;

  beforeAll(() => {
    process.env.CUBEJS_LOG_LEVEL = 'error';
  });

  afterAll(() => {
    if (prevLevel === undefined) {
      delete process.env.CUBEJS_LOG_LEVEL;
    } else {
      process.env.CUBEJS_LOG_LEVEL = prevLevel;
    }
  });

  test('non-error messages are not delivered to the custom logger when CUBEJS_LOG_LEVEL=error', async () => {
    const logger = jest.fn();
    const core = new CubejsServerCore({
      driverFactory: () => ({ type: 'postgres' }),
      apiSecret: 'secret',
      devServer: false,
      logger,
    });

    core.logger('Load Request', { requestId: 'r1', query: {} });
    core.logger('Query started', { requestId: 'r1' });
    core.logger('Error while querying', { requestId: 'r1', error: 'boom' });

    expect(logger.mock.calls.map(([msg]) => msg)).toEqual(['Error while querying']);

    await core.releaseConnections();
  });
});
