/* eslint-disable quotes */
import { PinotDriver } from '../../src/PinotDriver';

class TestPinotDriver extends PinotDriver {
  public override prepareQueryWithParams(query: string, values: unknown[]) {
    return super.prepareQueryWithParams(query, values);
  }
}

// Apache Pinot parses SQL with Calcite: a quote inside a string literal is
// escaped by doubling it and a backslash is plain data.
describe('PinotDriver SQL parameter escaping', () => {
  let driver: TestPinotDriver;

  beforeAll(() => {
    driver = new TestPinotDriver({
      host: 'localhost',
      port: '8099',
      dataSource: 'default',
    });
  });

  it('preserves LIKE escape sequences emitted by the schema compiler', () => {
    const sql = driver.prepareQueryWithParams(
      `SELECT * FROM orders WHERE LOWER(name) LIKE CONCAT('%', LOWER(?), '%') ESCAPE '\\'`,
      ['new\\_order\\%'],
    );

    expect(sql).toBe(
      `SELECT * FROM orders WHERE LOWER(name) LIKE CONCAT('%', LOWER('new\\_order\\%'), '%') ESCAPE '\\'`
    );
  });

  it('does not double literal backslashes in LIKE parameters', () => {
    const sql = driver.prepareQueryWithParams(
      `SELECT * FROM orders WHERE LOWER(name) LIKE CONCAT('%', LOWER(?), '%') ESCAPE '\\'`,
      ['folder\\\\name'],
    );

    expect(sql).toBe(
      `SELECT * FROM orders WHERE LOWER(name) LIKE CONCAT('%', LOWER('folder\\\\name'), '%') ESCAPE '\\'`
    );
  });

  it('doubles quotes so a value cannot break out of the literal', () => {
    const sql = driver.prepareQueryWithParams(
      'SELECT * FROM orders WHERE name = ?',
      [`o'reilly'); DROP TABLE orders; --`],
    );

    expect(sql).toBe(
      `SELECT * FROM orders WHERE name = 'o''reilly''); DROP TABLE orders; --'`
    );
  });

  it('keeps the literal closed for a quote payload', () => {
    // sqlstring rendered this as 'a\'' — Calcite reads the backslash as data,
    // so the doubled quote became an escaped quote and the literal ran on,
    // swallowing the rest of the statement.
    const sql = driver.prepareQueryWithParams(
      'SELECT * FROM orders WHERE name = ? AND status = ?',
      [`a'`, 'new'],
    );

    expect(sql).toBe(`SELECT * FROM orders WHERE name = 'a''' AND status = 'new'`);
  });

  it('keeps the literal closed for a value ending in a backslash', () => {
    const sql = driver.prepareQueryWithParams(
      'SELECT * FROM orders WHERE name = ? AND status = ?',
      ['payload\\', 'new'],
    );

    expect(sql).toBe(`SELECT * FROM orders WHERE name = 'payload\\' AND status = 'new'`);
  });

  it('keeps a literal percent sign in an equality parameter verbatim', () => {
    const sql = driver.prepareQueryWithParams(
      'SELECT * FROM orders WHERE discount_label = ?',
      ['100% cotton'],
    );

    expect(sql).toBe(`SELECT * FROM orders WHERE discount_label = '100% cotton'`);
  });

  it('escapes every element of an array parameter', () => {
    const sql = driver.prepareQueryWithParams(
      'SELECT * FROM orders WHERE status IN (?)',
      [[`it's`, 'b']],
    );

    expect(sql).toBe(`SELECT * FROM orders WHERE status IN ('it''s', 'b')`);
  });

  it('substitutes multiple placeholders in order', () => {
    const sql = driver.prepareQueryWithParams(
      `SELECT * FROM orders WHERE LOWER(name) LIKE CONCAT('%', LOWER(?), '%') ESCAPE '\\' AND status = ? AND amount > ?`,
      ['pending\\_review', 'new', 100],
    );

    expect(sql).toBe(
      `SELECT * FROM orders WHERE LOWER(name) LIKE CONCAT('%', LOWER('pending\\_review'), '%') ESCAPE '\\' AND status = 'new' AND amount > 100`
    );
  });
});
