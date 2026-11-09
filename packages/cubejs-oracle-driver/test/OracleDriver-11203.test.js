/* eslint-disable no-await-in-loop */
// https://github.com/cube-js/cube/issues/11203
// Pre-aggregation builds silently lose rows when a partition query returns
// more than 100,000 rows: the driver sets `oracledb.maxRows = 100000`, and
// node-oracledb's `connection.execute()` (non-ResultSet mode) stops fetching
// at `maxRows` without raising an error. `downloadQueryResults()` (used to
// load external rollups into Cube Store) and `query()` both go through
// `connection.execute()`, so anything beyond row 100,000 is dropped.

const oracledb = require('oracledb');
const { OracleDriver } = require('../driver/OracleDriver');

const TOTAL_ROWS = 150000;

/**
 * Fake connection that emulates node-oracledb's documented `execute()`
 * semantics: without a ResultSet, at most `options.maxRows` (falling back to
 * the global `oracledb.maxRows`; 0 = unlimited) rows are returned.
 */
function fakeConnection(totalRows) {
  return {
    execute: async (_sql, _binds, options = {}) => {
      const maxRows = options.maxRows !== undefined ? options.maxRows : oracledb.maxRows;
      const n = maxRows > 0 ? Math.min(maxRows, totalRows) : totalRows;
      const rows = new Array(n);

      for (let i = 0; i < n; i++) {
        rows[i] = { ID: i };
      }
      return { rows, metaData: [{ name: 'ID', dbTypeName: 'NUMBER' }] };
    },
  };
}

function createDriver(config = {}) {
  return new OracleDriver({
    host: 'localhost',
    db: 'FREEPDB1',
    user: 'cube',
    password: 'cube',
    ...config,
  });
}

describe('OracleDriver: large result sets (#11203)', () => {
  test('downloadQueryResults returns every row of a >100k-row pre-aggregation partition', async () => {
    const driver = createDriver();
    driver.withConnection = async (fn) => fn(fakeConnection(TOTAL_ROWS));

    const res = await driver.downloadQueryResults('SELECT id FROM t', [], {});

    expect(res.rows.length).toBe(TOTAL_ROWS);
    await driver.release();
  });
});

const ORACLE_HOST = process.env.CUBEJS_TEST_ORACLE_HOST;
const describeOracle = ORACLE_HOST ? describe : describe.skip;

describeOracle('OracleDriver against a real Oracle (#11203)', () => {
  let driver;

  beforeAll(() => {
    driver = createDriver({
      host: ORACLE_HOST,
      port: process.env.CUBEJS_TEST_ORACLE_PORT || 1521,
      db: process.env.CUBEJS_TEST_ORACLE_DB || 'FREEPDB1',
      user: process.env.CUBEJS_TEST_ORACLE_USER || 'cube',
      password: process.env.CUBEJS_TEST_ORACLE_PASS || 'cube',
    });
  });

  afterAll(async () => {
    await driver.release();
  });

  test('downloadQueryResults returns all rows', async () => {
    const res = await driver.downloadQueryResults(
      `SELECT level AS id FROM dual CONNECT BY level <= ${TOTAL_ROWS}`,
      [],
      {}
    );
    expect(res.rows.length).toBe(TOTAL_ROWS);
  }, 120000);
});
