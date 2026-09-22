/* globals describe,test,expect,beforeEach,afterAll */

import fs from 'fs';
import os from 'os';
import path from 'path';

import { getEnv } from '@cubejs-backend/shared';

import { ServerContainer } from '../src/server/container';

// `lookupConfiguration` is what resolves dev mode; with no cube.js in cwd it warns and
// returns the config it built, so it is safe to call directly
// `isCubeConfigEmpty` is protected; widen it rather than reaching in, so a change to
// the field is a compile error here instead of a silently passing test
class TestServerContainer extends ServerContainer {
  get cubeConfigEmpty() {
    return this.isCubeConfigEmpty;
  }

  // Poisoned before the call so that "never assigned" fails too, not just
  // "assigned from the wrong config" — the field now starts undefined, and undefined
  // would read as a pass against `toBe(true)` never being reached
  poisonCubeConfigEmpty() {
    this.isCubeConfigEmpty = false;
  }

  // The real loader uses a dynamic import, which this package's jest does not run with
  // --experimental-vm-modules. What matters here is what lookupConfiguration does with
  // the file's contents, not how they are read
  stubConfigurationFile(config) {
    this.loadConfigurationFromFile = async () => config;
  }
}

const makeContainer = (devMode) => new TestServerContainer({
  debug: false,
  ...(devMode !== undefined && { devMode }),
});

const lookupConfiguration = (devMode) => makeContainer(devMode).lookupConfiguration();

describe('ServerContainer dev mode resolution', () => {
  const saved = {
    CUBEJS_DEV_MODE: process.env.CUBEJS_DEV_MODE,
    CUBEJS_PG_SQL_PORT: process.env.CUBEJS_PG_SQL_PORT,
    CUBEJS_SQL_PORT: process.env.CUBEJS_SQL_PORT,
    CUBEJS_PRE_AGGREGATIONS_SCHEMA: process.env.CUBEJS_PRE_AGGREGATIONS_SCHEMA,
    NODE_ENV: process.env.NODE_ENV,
  };

  beforeEach(() => {
    delete process.env.CUBEJS_DEV_MODE;
    delete process.env.CUBEJS_PG_SQL_PORT;
    delete process.env.CUBEJS_SQL_PORT;
    delete process.env.CUBEJS_PRE_AGGREGATIONS_SCHEMA;
    // lookupConfiguration writes NODE_ENV=development, which would otherwise carry
    // into every case after the first
    delete process.env.NODE_ENV;
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

  // server-core reads this as "nothing is configured yet" and opens Playground's
  // connection wizard on it, so folding the devServer default in before measuring
  // would silently send a fresh project to the query builder instead
  test('the devServer default does not make an empty config look configured', async () => {
    const container = makeContainer(true);
    container.poisonCubeConfigEmpty();

    const config = await container.lookupConfiguration();

    // The resolved config is not empty, yet the project still counts as unconfigured
    expect(config.devServer).toBe(true);
    expect(Object.keys(config).length).toBeGreaterThan(0);
    expect(container.cubeConfigEmpty).toBe(true);
  });

  // OptsHandler pins it, from the config that wins rather than the command's request
  test('`cubejs dev-server` does not pin the pre-aggregation schema itself', async () => {
    await lookupConfiguration(true);

    expect(process.env.CUBEJS_PRE_AGGREGATIONS_SCHEMA).toBeUndefined();
  });

  // `cube.js` is loaded after the command's request is resolved, and `...userConfig`
  // wins, so anything keyed on the request rather than on the resolved config would
  // leave this instance in dev mode while server-core puts it in production mode
  test('a cube.js devServer: false wins over the command', async () => {
    const projectDir = fs.mkdtempSync(path.join(os.tmpdir(), 'cube-container-'));
    const cwd = process.cwd();

    fs.writeFileSync(
      path.join(projectDir, 'cube.js'),
      'module.exports = { devServer: false };\n'
    );

    try {
      process.chdir(projectDir);

      const container = makeContainer(true);
      container.stubConfigurationFile({ devServer: false });

      const config = await container.lookupConfiguration();

      expect(config.devServer).toBe(false);
      // The sync is written before `cube.js` is loaded, so it is keyed on the command's
      // request; leaving `development` here would hand `gracefulShutdown`,
      // `refreshWorkerMode` and `detectQueueAndCacheDriver` a dev server that is not one
      expect(process.env.NODE_ENV).toBeUndefined();
    } finally {
      process.chdir(cwd);
      fs.rmSync(projectDir, { recursive: true, force: true });
    }
  });

  test('a cube.js devServer: false restores the NODE_ENV it found', async () => {
    process.env.NODE_ENV = 'production';

    const container = makeContainer(true);
    container.stubConfigurationFile({ devServer: false });

    const projectDir = fs.mkdtempSync(path.join(os.tmpdir(), 'cube-container-'));
    const cwd = process.cwd();

    fs.writeFileSync(
      path.join(projectDir, 'cube.js'),
      'module.exports = { devServer: false };\n'
    );

    try {
      process.chdir(projectDir);

      await container.lookupConfiguration();

      // Restored rather than deleted: the command overwrote a value the user set, and
      // `gracefulShutdown` reads exactly this one — 30 seconds in production, 2 outside
      expect(process.env.NODE_ENV).toEqual('production');
    } finally {
      process.chdir(cwd);
      fs.rmSync(projectDir, { recursive: true, force: true });
    }
  });

  // The restore is scoped to this method's own write: deleting a NODE_ENV that cube.js
  // set would drop gracefulShutdown from 30 seconds to 2 and swap
  // detectQueueAndCacheDriver from cubestore to the memory queue
  test('leaves a NODE_ENV this run never wrote alone', async () => {
    const container = makeContainer(true);
    container.stubConfigurationFile({});

    const projectDir = fs.mkdtempSync(path.join(os.tmpdir(), 'cube-container-'));
    const cwd = process.cwd();

    fs.writeFileSync(path.join(projectDir, 'cube.js'), 'module.exports = {};\n');

    try {
      process.chdir(projectDir);

      // `cubejs dev-server` with dev mode explicitly off: nothing writes NODE_ENV here,
      // so the cube.js assignment below is the only one, and it is not this run's to undo
      process.env.CUBEJS_DEV_MODE = 'false';
      container.stubConfigurationFile({});
      const loader = container.loadConfigurationFromFile.bind(container);
      container.loadConfigurationFromFile = async () => {
        process.env.NODE_ENV = 'production';

        return loader();
      };

      await container.lookupConfiguration();

      expect(process.env.NODE_ENV).toEqual('production');
    } finally {
      process.chdir(cwd);
      fs.rmSync(projectDir, { recursive: true, force: true });
    }
  });

  test('leaves a NODE_ENV cube.js chose over the one this run wrote', async () => {
    const container = makeContainer(true);

    const projectDir = fs.mkdtempSync(path.join(os.tmpdir(), 'cube-container-'));
    const cwd = process.cwd();

    fs.writeFileSync(
      path.join(projectDir, 'cube.js'),
      'module.exports = { devServer: false };\n'
    );

    try {
      process.chdir(projectDir);

      // The run writes `development`, cube.js replaces it and turns the dev server off.
      // The restore is scoped to the value it wrote, so the file's choice survives
      container.loadConfigurationFromFile = async () => {
        process.env.NODE_ENV = 'staging';

        return { devServer: false };
      };

      await container.lookupConfiguration();

      expect(process.env.NODE_ENV).toEqual('staging');
    } finally {
      process.chdir(cwd);
      fs.rmSync(projectDir, { recursive: true, force: true });
    }
  });

  // SIGUSR1 rebuilds the core through lookupConfiguration(true). The pin refuses to
  // overwrite a non-empty value, so without the release the drivers keep the schema the
  // previous config resolved while the new instance names a different one
  test('a reload releases the pin so the next config can take it', async () => {
    const { pinPreAggregationsSchema } = require('@cubejs-backend/shared');

    pinPreAggregationsSchema('dev_pre_aggregations');
    expect(process.env.CUBEJS_PRE_AGGREGATIONS_SCHEMA).toEqual('dev_pre_aggregations');

    await makeContainer().lookupConfiguration(true);

    expect(process.env.CUBEJS_PRE_AGGREGATIONS_SCHEMA).toBeUndefined();
  });

  test('a reload leaves a CUBEJS_PRE_AGGREGATIONS_SCHEMA the user set alone', async () => {
    process.env.CUBEJS_PRE_AGGREGATIONS_SCHEMA = 'my_schema';

    await makeContainer().lookupConfiguration(true);

    expect(process.env.CUBEJS_PRE_AGGREGATIONS_SCHEMA).toEqual('my_schema');
  });

  test('`cubejs server` asks for nothing', async () => {
    const config = await lookupConfiguration();

    expect(config.devServer).toBeUndefined();
    expect(process.env.CUBEJS_DEV_MODE).toBeUndefined();
    expect(process.env.CUBEJS_PRE_AGGREGATIONS_SCHEMA).toBeUndefined();
    expect(getEnv('pgSqlPort')).toBeUndefined();
  });
});
