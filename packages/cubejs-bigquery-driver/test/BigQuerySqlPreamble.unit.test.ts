import type { Query } from '@google-cloud/bigquery';
import { BigQueryDriver } from '../src';

// The BigQuery client is not mockable, so the prepend is tested on the driver's
// own method with the constructor bypassed — no credentials involved.
class BigQueryDriverOpen extends BigQueryDriver {
  public override withSqlPreamble(bigQueryQuery: Query): Query {
    return super.withSqlPreamble(bigQueryQuery);
  }
}

const driverWith = (preamble?: string): BigQueryDriverOpen => {
  const driver = Object.create(BigQueryDriverOpen.prototype) as BigQueryDriverOpen;
  (driver as any).sqlPreamble = () => preamble;
  return driver;
};

const TEMP_FN = 'CREATE TEMP FUNCTION double_it(x INT64) AS (x * 2)';

describe('BigQueryDriver.withSqlPreamble', () => {
  test('prepends the preamble into the query text', () => {
    const result = driverWith(TEMP_FN).withSqlPreamble({ query: 'SELECT double_it(21)' });

    expect(result.query).toEqual(`${TEMP_FN};\nSELECT double_it(21)`);
  });

  test('leaves the query untouched when no preamble is set', () => {
    const query: Query = { query: 'SELECT 1', useLegacySql: false };

    expect(driverWith(undefined).withSqlPreamble(query)).toBe(query);
  });

  test('preserves the rest of the job request', () => {
    const result = driverWith(TEMP_FN).withSqlPreamble({
      query: 'SELECT 1',
      params: [1, 2],
      parameterMode: 'positional',
      useLegacySql: false,
    });

    expect(result.params).toEqual([1, 2]);
    expect(result.parameterMode).toEqual('positional');
    expect(result.useLegacySql).toBe(false);
  });

  test('tolerates a job request with no query text', () => {
    const query = { params: [] } as Query;

    expect(driverWith(TEMP_FN).withSqlPreamble(query)).toBe(query);
  });

  // A multi-statement request is a SCRIPT, and a script job ignores the
  // destination table — so a pre-aggregation build would report success while
  // writing no rows. Refusing loudly beats silently producing an empty table.
  describe('pre-aggregation (destination) jobs', () => {
    const destinationJob = (query: string): Query => ({
      query,
      destination: { id: 'target' } as any,
      createDisposition: 'CREATE_IF_NEEDED',
    });

    test('allows a CREATE TEMP FUNCTION preamble, the one script-exempt shape', () => {
      const result = driverWith(TEMP_FN).withSqlPreamble(destinationJob('CREATE TABLE t AS SELECT 1'));

      expect(result.query).toEqual(`${TEMP_FN};\nCREATE TABLE t AS SELECT 1`);
      expect(result.destination).toBeDefined();
    });

    test('allows several CREATE TEMP FUNCTION statements', () => {
      const preamble = `${TEMP_FN}; CREATE OR REPLACE TEMPORARY FUNCTION triple(x INT64) AS (x * 3)`;
      const result = driverWith(preamble).withSqlPreamble(destinationJob('CREATE TABLE t AS SELECT 1'));

      expect(result.query).toContain('double_it');
      expect(result.query).toContain('triple');
    });

    test('refuses a SET preamble rather than silently dropping the destination', () => {
      expect(() => driverWith("SET @@dataset_id = 'analytics'")
        .withSqlPreamble(destinationJob('CREATE TABLE t AS SELECT 1')))
        .toThrow(/cannot be applied to a pre-aggregation build/);
    });

    test('refuses a preamble that mixes a temp function with another statement', () => {
      expect(() => driverWith(`${TEMP_FN}; SET @@dataset_id = 'analytics'`)
        .withSqlPreamble(destinationJob('CREATE TABLE t AS SELECT 1')))
        .toThrow(/cannot be applied to a pre-aggregation build/);
    });

    test('still allows the same non-exempt preamble on a normal query job', () => {
      const result = driverWith("SET @@dataset_id = 'analytics'")
        .withSqlPreamble({ query: 'SELECT 1' });

      expect(result.query).toEqual("SET @@dataset_id = 'analytics';\nSELECT 1");
    });
  });
});
