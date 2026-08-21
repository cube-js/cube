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
      expect(() => driverWith('SET @@dataset_id = \'analytics\'')
        .withSqlPreamble(destinationJob('CREATE TABLE t AS SELECT 1')))
        .toThrow(/cannot be applied to a pre-aggregation build/);
    });

    test('refuses a preamble that mixes a temp function with another statement', () => {
      expect(() => driverWith(`${TEMP_FN}; SET @@dataset_id = 'analytics'`)
        .withSqlPreamble(destinationJob('CREATE TABLE t AS SELECT 1')))
        .toThrow(/cannot be applied to a pre-aggregation build/);
    });

    test('still allows the same non-exempt preamble on a normal query job', () => {
      const result = driverWith('SET @@dataset_id = \'analytics\'')
        .withSqlPreamble({ query: 'SELECT 1' });

      expect(result.query).toEqual('SET @@dataset_id = \'analytics\';\nSELECT 1');
    });
  });

  // stream() does not go through withSqlPreamble — it builds its own request and
  // prepends directly. Everything above would stay green with the stream path's
  // prepend deleted, so it needs the request BigQuery actually receives.
  describe('the stream path', () => {
    const streamingDriverWith = (preamble?: string) => {
      const driver = driverWith(preamble) as any;
      const requests: { query: string }[] = [];

      driver.bigquery = {
        createQueryStream: async (request: { query: string }) => {
          requests.push(request);
          return { pipe: () => { /* the row stream is not under test */ } };
        },
      };
      driver.buildQueryLabels = () => undefined;

      return { driver, requests };
    };

    test('prepends the preamble into the streamed query', async () => {
      const { driver, requests } = streamingDriverWith(TEMP_FN);

      await driver.stream('SELECT double_it(21)', [], {});

      expect(requests).toHaveLength(1);
      expect(requests[0].query).toEqual(`${TEMP_FN};\nSELECT double_it(21)`);
    });

    test('leaves the streamed query untouched when no preamble is configured', async () => {
      const { driver, requests } = streamingDriverWith(undefined);

      await driver.stream('SELECT 1', [], {});

      expect(requests[0].query).toEqual('SELECT 1');
    });
  });
});

describe('the script-exempt shape tolerates comments', () => {
  const destinationJobFor = (query: string) => ({
    query,
    destination: { id: 'stb_pre_aggregations.orders' } as any,
  });

  test('a line-commented temp function is still allowed on a destination job', () => {
    const preamble = '-- Median helper, see the data model\nCREATE TEMP FUNCTION median(x INT64) AS (x)';

    expect(() => driverWith(preamble).withSqlPreamble(destinationJobFor('CREATE TABLE t AS SELECT 1')))
      .not.toThrow();
  });

  test('a block-commented temp function is still allowed', () => {
    const preamble = '/* Median helper */ CREATE TEMP FUNCTION median(x INT64) AS (x)';

    expect(() => driverWith(preamble).withSqlPreamble(destinationJobFor('CREATE TABLE t AS SELECT 1')))
      .not.toThrow();
  });

  test('a comment does not smuggle a non-exempt statement past the check', () => {
    const preamble = '-- looks harmless\nSET @@dataset_id = \'analytics\'';

    expect(() => driverWith(preamble).withSqlPreamble(destinationJobFor('CREATE TABLE t AS SELECT 1')))
      .toThrow(/cannot be applied to a pre-aggregation build/);
  });

  // A trailing comment must not cost a legitimate UDF preamble its exemption:
  // the splitter used to emit the comment as its own statement, which reduced to
  // an empty one and failed the CREATE TEMP FUNCTION test.
  test('a temp function followed by a comment keeps its exemption', () => {
    const preamble = 'CREATE TEMP FUNCTION median(x INT64) AS (x);\n-- keep in sync with the data model';

    expect(() => driverWith(preamble).withSqlPreamble(destinationJobFor('CREATE TABLE t AS SELECT 1')))
      .not.toThrow();
  });
});

// The splitter hands back the whole blob as one entry when it cannot find the
// boundaries, and that entry may contain several statements. Judging it exempt
// because it merely STARTS with CREATE TEMP FUNCTION would let a script onto a
// destination job — which BigQuery runs as a script, ignoring the destination
// table, so the build reports success having written no rows.
describe('the script-exempt shape fails closed on an ambiguous preamble', () => {
  const destinationJobFor = (query: string) => ({
    query,
    destination: { id: 'stb_pre_aggregations.orders' } as any,
  });

  test.each([
    [
      'a nested block comment',
      'CREATE TEMP FUNCTION f(x INT64) AS (x); /* uses /* nested */ SET @@dataset_id = \'analytics\'',
    ],
    [
      'an unterminated literal',
      'CREATE TEMP FUNCTION f(x INT64) AS (x); SET @@dataset_id = \'unterminated',
    ],
    [
      'a dialect-dependent quote escape',
      'CREATE TEMP FUNCTION f(x STRING) AS (x); SET @@dataset_id = \'a\\\'; SET @@x = \'b\'',
    ],
  ])('refuses %s on a destination job', (_name, preamble) => {
    expect(() => driverWith(preamble).withSqlPreamble(destinationJobFor('CREATE TABLE t AS SELECT 1')))
      .toThrow(/cannot be applied to a pre-aggregation build/);
  });

  // An ambiguous preamble is still fine on a normal query: no destination table
  // is at stake, and BigQuery's own parser is authoritative.
  test('still applies an ambiguous preamble to a non-destination query', () => {
    const preamble = 'CREATE TEMP FUNCTION f(x INT64) AS (x); /* uses /* nested */ SET @@x = 1';

    expect(driverWith(preamble).withSqlPreamble({ query: 'SELECT f(1)' }).query)
      .toEqual(`${preamble};\nSELECT f(1)`);
  });
});
