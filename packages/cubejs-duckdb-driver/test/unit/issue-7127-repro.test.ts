import { DuckDBDriver } from '../../src';

// https://github.com/cube-js/cube/issues/7127
// DuckDB's SUM() over INTEGER (or an explicit ::HUGEINT) yields HUGEINT. When a pre-aggregation
// is built, the table is created in DuckDB (CTAS) and its column types are read back via
// tableColumnTypes() and sent to Cube Store, which rejects `hugeint`:
//   "Custom type 'hugeint' is not supported"
// The driver must map HUGEINT to a generic type Cube Store understands.
const CUBESTORE_SUPPORTED_NUMERIC = ['bigint', 'decimal', 'decimal(38,0)', 'decimal(38, 0)'];

describe('DuckDBDriver issue #7127: HUGEINT in pre-aggregation column types', () => {
  let driver: DuckDBDriver;

  jest.setTimeout(60 * 1000);

  beforeAll(async () => {
    driver = new DuckDBDriver({});
    await driver.query('CREATE SCHEMA IF NOT EXISTS issue_7127;', []);
    await driver.query(
      'CREATE TABLE issue_7127.pre_agg AS SELECT org, SUM(metric) AS metric_sum, SUM(huge) AS huge_sum ' +
      'FROM (SELECT \'foo\' AS org, 10::INT AS metric, 10::HUGEINT AS huge) AS t GROUP BY org',
      []
    );
  });

  afterAll(async () => {
    await driver.query('DROP SCHEMA IF EXISTS issue_7127 CASCADE;', []);
    await driver.release();
  });

  test('SUM over INTEGER produces HUGEINT in DuckDB', async () => {
    const [row] = await driver.query<{ t: string }>(
      'SELECT typeof(metric_sum) AS t FROM issue_7127.pre_agg',
      []
    );
    expect(row.t.toLowerCase()).toEqual('hugeint');
  });

  test('tableColumnTypes maps HUGEINT to a Cube Store-supported type', async () => {
    const types = await driver.tableColumnTypes('issue_7127.pre_agg');

    expect(types.find(c => c.name === 'org')?.type).toEqual('text');

    for (const name of ['metric_sum', 'huge_sum']) {
      const type = types.find(c => c.name === name)?.type;
      expect(type).not.toEqual('hugeint');
      expect(CUBESTORE_SUPPORTED_NUMERIC).toContain(type);
    }
  });
});
