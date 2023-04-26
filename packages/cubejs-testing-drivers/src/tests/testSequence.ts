import { jest, expect, beforeAll, afterAll } from '@jest/globals';
import { Environment } from '../types/Environment';
import { PatchedDriver } from '../types/PatchedDriver';
import { CubejsServerCoreExposed } from '../types/CubejsServerCoreExposed';
import {
  getFixtures,
  getCreateQueries,
  getCore,
  getDriver,
  patchDriver,
  hookPreaggs,
  runEnvironment,
} from '../helpers';

export function testSequence(type: string): void {
  describe(`Sequence with the @cubejs-backend/${type}-driver`, () => {
    jest.setTimeout(60 * 5 * 1000);

    const fixtures = getFixtures(type);
    let core: CubejsServerCoreExposed;
    let source: PatchedDriver;
    let storage: PatchedDriver;
    let query: string[];
    let env: Environment;

    function execute(name: string, test: () => Promise<void>) {
      if (fixtures.skip && fixtures.skip.indexOf(name) >= 0) {
        it.skip(name, test);
      } else {
        it(name, test);
      }
    }

    beforeAll(async () => {
      env = await runEnvironment(type, 'core');
      process.env.CUBEJS_REFRESH_WORKER = 'true';
      process.env.CUBEJS_CUBESTORE_HOST = '127.0.0.1';
      process.env.CUBEJS_CUBESTORE_PORT = `${env.store.port}`;
      process.env.CUBEJS_CUBESTORE_USER = 'root';
      process.env.CUBEJS_CUBESTORE_PASS = 'root';
      process.env.CUBEJS_CACHE_AND_QUEUE_DRIVER = 'memory'; // memory, cubestore
      if (env.data) {
        process.env.CUBEJS_DB_HOST = '127.0.0.1';
        process.env.CUBEJS_DB_PORT = `${env.data.port}`;
      }
      const drivers = await getDriver(type);
      source = drivers.source;
      storage = drivers.storage;
      query = getCreateQueries(type, 'core');
      await Promise.all(query.map(async (q) => {
        await source.query(q);
      }));
      patchDriver(source);
      patchDriver(storage);
      core = getCore(type, 'cubestore', source, storage);
    });

    afterAll(async () => {
      const tables = Object
        .keys(fixtures.tables)
        .map((key: string) => `${fixtures.tables[
            <'products' | 'customers' | 'ecommerce'>key
        ]}_core`);
      await Promise.all(
        tables.map(async (t) => {
          await source.dropTable(t);
        })
      );
      await source.release();
      await storage.release();
      await core.shutdown();
      await env.stop();
    });

    beforeEach(() => {
      source.calls = [];
      storage.calls = [];
    });

    execute('for the Customers.RollingExternal', async () => {
      await hookPreaggs(core, 'Customers.RAExternal');
      expect([source.calls, storage.calls]).toMatchSnapshot();
    });

    execute('for the Customers.RollingInternal', async () => {
      await hookPreaggs(core, 'Customers.RAInternal');
      expect([source.calls, storage.calls]).toMatchSnapshot();
    });

    execute('for the ECommerce.SimpleAnalysisExternal', async () => {
      await hookPreaggs(core, 'ECommerce.SAExternal');
      expect([source.calls, storage.calls]).toMatchSnapshot();
    });

    execute('for the ECommerce.SimpleAnalysisInternal', async () => {
      await hookPreaggs(core, 'ECommerce.SAInternal');
      expect([source.calls, storage.calls]).toMatchSnapshot();
    });

    execute('for the ECommerce.TimeAnalysisExternal', async () => {
      await hookPreaggs(core, 'ECommerce.TAExternal');
      expect([source.calls, storage.calls]).toMatchSnapshot();
    });

    execute('for the ECommerce.TimeAnalysisInternal', async () => {
      await hookPreaggs(core, 'ECommerce.TAInternal');
      expect([source.calls, storage.calls]).toMatchSnapshot();
    });
  });
}
