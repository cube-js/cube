/* globals describe,test,expect,beforeEach,afterAll */

import { getEnv } from '@cubejs-backend/shared';

import { ServerContainer } from '../src/server/container';

// `lookupConfiguration` is what resolves dev mode; with no cube.js in cwd it warns and
// returns the config it built, so it is safe to call directly
const lookupConfiguration = (devMode) => new ServerContainer({
  debug: false,
  ...(devMode !== undefined && { devMode }),
}).lookupConfiguration();

describe('ServerContainer dev mode resolution', () => {
  const saved = {
    CUBEJS_DEV_MODE: process.env.CUBEJS_DEV_MODE,
    CUBEJS_PG_SQL_PORT: process.env.CUBEJS_PG_SQL_PORT,
    CUBEJS_SQL_PORT: process.env.CUBEJS_SQL_PORT,
    NODE_ENV: process.env.NODE_ENV,
  };

  beforeEach(() => {
    delete process.env.CUBEJS_DEV_MODE;
    delete process.env.CUBEJS_PG_SQL_PORT;
    delete process.env.CUBEJS_SQL_PORT;
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

  test('`cubejs dev-server` asks for dev mode through CreateOptions.devServer', async () => {
    const config = await lookupConfiguration(true);

    expect(config.devServer).toBe(true);
    expect(process.env.NODE_ENV).toEqual('development');
  });

  test('`cubejs dev-server` does not write CUBEJS_DEV_MODE', async () => {
    await lookupConfiguration(true);

    // The variable also gates the SQL API's default port and its password check, so
    // writing it would serve an unauthenticated SQL API wherever a port is configured
    expect(process.env.CUBEJS_DEV_MODE).toBeUndefined();
    expect(getEnv('devMode')).toBe(false);
  });

  test('`cubejs dev-server` leaves every SQL API port source alone', async () => {
    await lookupConfiguration(true);

    // Nothing configured: no SQL API, because `pgSqlPort` keys off CUBEJS_DEV_MODE
    expect(getEnv('pgSqlPort')).toBeUndefined();
    expect(process.env.CUBEJS_PG_SQL_PORT).toBeUndefined();
    expect(process.env.CUBEJS_SQL_PORT).toBeUndefined();
  });

  test('a configured SQL API port still gets the production credentials path', async () => {
    process.env.CUBEJS_SQL_PORT = '13306';
    process.env.CUBEJS_PG_SQL_PORT = '15432';

    await lookupConfiguration(true);

    // `getEnv('devMode')` false is what makes sql-server generate a user and password
    expect(getEnv('devMode')).toBe(false);
    expect(getEnv('sqlPort')).toEqual(13306);
    expect(getEnv('pgSqlPort')).toEqual(15432);
  });

  test('an explicit CUBEJS_DEV_MODE=true still turns the SQL API on, as before', async () => {
    process.env.CUBEJS_DEV_MODE = 'true';

    const config = await lookupConfiguration(true);

    // The user asked for dev mode by name, so the env var drives it and the SQL API
    // defaults to 15432 exactly as it did before this change
    expect(config.devServer).toBeUndefined();
    expect(getEnv('devMode')).toBe(true);
    expect(getEnv('pgSqlPort')).toEqual(15432);
  });

  test('an explicit CUBEJS_DEV_MODE=false wins over the dev server', async () => {
    process.env.CUBEJS_DEV_MODE = 'false';

    const config = await lookupConfiguration(true);

    expect(config.devServer).toBeUndefined();
    expect(getEnv('devMode')).toBe(false);
  });

  test('`cubejs server` asks for nothing', async () => {
    const config = await lookupConfiguration();

    expect(config.devServer).toBeUndefined();
    expect(process.env.CUBEJS_DEV_MODE).toBeUndefined();
    expect(getEnv('pgSqlPort')).toBeUndefined();
  });
});
