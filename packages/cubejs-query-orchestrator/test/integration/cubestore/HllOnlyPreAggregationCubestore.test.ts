// A rollup whose only members are `count_distinct_approx` measures is stored in Cube Store as a
// table with only HLL columns, which can't be part of a sort key.
// @link https://github.com/cube-js/cube/issues/8916

import { CubeStoreDevDriver, CubeStoreDriver, CubeStoreHandler } from '@cubejs-backend/cubestore-driver';

let cubeStoreHandler: CubeStoreHandler | undefined;

if ((process.env.CUBEJS_TESTING_CUBESTORE_AUTO_PROVISIONING || 'false') === 'true') {
  cubeStoreHandler = new CubeStoreHandler({
    stdout: (data) => console.log(data.toString().trim()),
    stderr: (data) => console.log(data.toString().trim()),
    onRestart: (code) => console.log({ warning: `Instance exit with ${code}, restarting` }),
  });
}

describe('Cube Store table with only HLL columns', () => {
  jest.setTimeout(60 * 1000);

  let driver: CubeStoreDriver;
  const schema = `test_hll_only_${Date.now()}`;

  beforeAll(async () => {
    if (cubeStoreHandler) {
      await cubeStoreHandler.acquire();
    }
    driver = cubeStoreHandler ? new CubeStoreDevDriver(cubeStoreHandler) : new CubeStoreDriver({});
    await driver.createSchemaIfNotExists(schema);
  });

  afterAll(async () => {
    await driver.query(`DROP SCHEMA ${schema}`, []).catch((): undefined => undefined);
    await driver.release();
    if (cubeStoreHandler) {
      await cubeStoreHandler.release(true);
    }
  });

  // The same statement Cube issues for `pre_aggregations: { main: { measures: [mes] } }`
  // with `mes` of type `count_distinct_approx` on Postgres (HLL extension).
  test.each(['HLL_POSTGRES', 'HYPERLOGLOG', 'HYPERLOGLOGPP'])('creates a table with a single %s column', async (type) => {
    const table = `${schema}.my_cube_test_main_${type.toLowerCase()}`;

    await expect(
      driver.query(`CREATE TABLE ${table} (\`my_cube_test__mes\` ${type})`, [])
    ).resolves.toBeDefined();

    expect(await driver.query(`SELECT cardinality(merge(\`my_cube_test__mes\`)) FROM ${table}`, [])).toBeDefined();
  });

  // Control: adding a non-HLL measure (the workaround from the issue) works today.
  test('creates a table with an HLL column next to a count column', async () => {
    const table = `${schema}.my_cube_test_main_with_count`;

    await expect(
      driver.query(`CREATE TABLE ${table} (\`my_cube_test__mes\` HLL_POSTGRES, \`my_cube_test__cnt\` bigint)`, [])
    ).resolves.toBeDefined();
  });
});
