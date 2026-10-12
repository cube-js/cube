// https://github.com/cube-js/cube/issues/7127
// A `sum` measure over an integer column is HUGEINT in DuckDB. When the pre-aggregation is
// copied to Cube Store, the column type is read back through `tableColumnTypes` and passed to
// Cube Store as-is, so Cube Store fails with "Custom type 'HUGEINT' is not supported".
import { DuckDBDriver } from '../../src';

// Generic types that Cube Store's CREATE TABLE understands for numbers.
const CUBESTORE_NUMERIC_TYPES = ['int', 'bigint', 'decimal', 'double', 'float'];

describe('issue-7127: DuckDB HUGEINT pre-aggregation column', () => {
  let driver: DuckDBDriver;

  jest.setTimeout(60 * 1000);

  beforeAll(async () => {
    driver = new DuckDBDriver({});
    await driver.createSchemaIfNotExists('prod_pre_aggregations');
  });

  afterAll(async () => {
    await driver.release();
  });

  test.each([
    ['10::hugeint', 'spam_main_hugeint'],
    // The reporter's workaround (cast to int before summing) still yields HUGEINT
    ['10::int', 'spam_main_int'],
  ])('sum(%s) column maps to a Cube Store compatible generic type', async (metricSql, tableName) => {
    const table = `prod_pre_aggregations.${tableName}`;
    // What the pre-aggregation build runs against DuckDB before downloading it into Cube Store
    await driver.loadPreAggregationIntoTable(
      table,
      `CREATE TABLE ${table} AS SELECT "spam".org "spam__org", sum("spam".metric) "spam__metric" ` +
      `FROM (SELECT 'foo' AS org, ${metricSql} AS metric) AS "spam" GROUP BY 1`,
      [],
      {}
    );

    const [typeRow] = await driver.query<{ t: string }>(`SELECT typeof(spam__metric) AS t FROM ${table}`, []);
    expect(typeRow.t.toUpperCase()).toBe('HUGEINT');

    // PreAggregationLoader#getTableDataWithTempTable streams the table and takes its types from here
    const types = await driver.tableColumnTypes(table);
    const metric = types.find(c => c.name === 'spam__metric');
    console.log(types);
    expect(CUBESTORE_NUMERIC_TYPES).toContain(metric?.type);
  });
});
