/* eslint-disable @typescript-eslint/no-empty-function */

// https://github.com/cube-js/cube/issues/2856
// CUBEJS_LOG_LEVEL is ignored when a custom `logger` is set in the configuration:
// the custom logger receives every message, including `info`/`trace` ones, and
// `params` carries no `level` it could filter by.

import { dropPreAggregationsSchemaPin } from '@cubejs-backend/shared';

import { CubejsServerCore } from '../../src';

describe('Issue #2856: CUBEJS_LOG_LEVEL with a custom logger', () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    process.env.CUBEJS_DB_TYPE = 'mysql';
    process.env.CUBEJS_API_SECRET = 'secret';
    process.env.CUBEJS_LOG_LEVEL = 'error';
    delete process.env.CUBEJS_DEV_MODE;
    dropPreAggregationsSchemaPin();
  });

  afterEach(() => {
    process.env = { ...originalEnv };
  });

  test('a custom logger only receives messages at or above CUBEJS_LOG_LEVEL', async () => {
    const logger = jest.fn((_message: string, _params: any) => {});

    const core = new CubejsServerCore({ logger });
    core.logger('Load Request', { query: { measures: ['Orders.count'] } });
    core.logger('Performing query', { requestId: 'r1' });
    core.logger('Cube Store is not supported on your system', { warning: 'Some warning' });
    core.logger('Error querying db', { error: 'Error: division by zero' });
    await core.beforeShutdown();
    await core.shutdown();

    expect(logger.mock.calls.map(([message]) => message)).toEqual(['Error querying db']);
  });
});
