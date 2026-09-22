/* globals describe,test,expect,beforeEach,afterAll */

import { ServerContainer } from '../src/server/container';

// `lookupConfiguration` is what does the dev mode defaulting; with no cube.js in cwd it
// warns and returns {}, so it is safe to call directly
const resolveDevMode = (devMode) => new ServerContainer({
  debug: false,
  ...(devMode !== undefined && { devMode }),
}).lookupConfiguration();

describe('ServerContainer dev mode defaulting', () => {
  const saved = {
    CUBEJS_DEV_MODE: process.env.CUBEJS_DEV_MODE,
    CUBEJS_PG_SQL_PORT: process.env.CUBEJS_PG_SQL_PORT,
    NODE_ENV: process.env.NODE_ENV,
  };

  beforeEach(() => {
    delete process.env.CUBEJS_DEV_MODE;
    delete process.env.CUBEJS_PG_SQL_PORT;
  });

  afterAll(() => {
    Object.entries(saved).forEach(([key, value]) => {
      if (value === undefined) {
        delete process.env[key];
      } else {
        process.env[key] = value;
      }
    });
  });

  test('`cubejs dev-server` turns dev mode on when nothing else set it', async () => {
    await resolveDevMode(true);

    expect(process.env.CUBEJS_DEV_MODE).toEqual('true');
    expect(process.env.NODE_ENV).toEqual('development');
  });

  test('`cubejs dev-server` leaves the SQL API off unless it was asked for', async () => {
    await resolveDevMode(true);

    // Without this the dev mode default above would bind an unauthenticated Postgres
    // listener on 15432, which `cubejs dev-server` never opened before
    expect(process.env.CUBEJS_PG_SQL_PORT).toEqual('false');
  });

  test('an explicit CUBEJS_PG_SQL_PORT still wins over the dev server', async () => {
    process.env.CUBEJS_PG_SQL_PORT = '15433';

    await resolveDevMode(true);

    expect(process.env.CUBEJS_PG_SQL_PORT).toEqual('15433');
  });

  test('an explicit CUBEJS_DEV_MODE=false wins over the dev server', async () => {
    process.env.CUBEJS_DEV_MODE = 'false';

    await resolveDevMode(true);

    expect(process.env.CUBEJS_DEV_MODE).toEqual('false');
    // The port is only defaulted alongside a dev mode this container set itself
    expect(process.env.CUBEJS_PG_SQL_PORT).toBeUndefined();
  });

  test('`cubejs server` does not turn dev mode on', async () => {
    await resolveDevMode();

    expect(process.env.CUBEJS_DEV_MODE).toBeUndefined();
    expect(process.env.CUBEJS_PG_SQL_PORT).toBeUndefined();
  });

  test('an explicit CUBEJS_DEV_MODE=true keeps the SQL API default it had on master', async () => {
    process.env.CUBEJS_DEV_MODE = 'true';

    await resolveDevMode();

    expect(process.env.CUBEJS_PG_SQL_PORT).toBeUndefined();
  });
});
