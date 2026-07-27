/* eslint-disable quotes */
import { PrestoDriver } from '../../src/PrestoDriver';

class TestPrestoDriver extends PrestoDriver {
  public override prepareQueryWithParams(query: string, values: unknown[]) {
    return super.prepareQueryWithParams(query, values);
  }
}

describe('PrestoDriver SQL parameter escaping', () => {
  let driver: TestPrestoDriver;

  beforeAll(() => {
    driver = new TestPrestoDriver({
      host: 'localhost',
      port: '8080',
      catalog: 'test',
      schema: 'default',
      dataSource: 'default',
    });
  });

  it('preserves LIKE escape sequences emitted by the schema compiler', () => {
    // The exact shape PrestodbQuery renders for a `contains` filter over the
    // value `new_order%` (wildcards pre-escaped to `new\_order\%`).
    const sql = driver.prepareQueryWithParams(
      `SELECT * FROM orders WHERE LOWER(name) LIKE CONCAT('%', LOWER(?), '%') ESCAPE '\\'`,
      ['new\\_order\\%'],
    );

    expect(sql).toBe(
      `SELECT * FROM orders WHERE LOWER(name) LIKE CONCAT('%', LOWER('new\\_order\\%'), '%') ESCAPE '\\'`
    );
  });

  it('does not double literal backslashes in LIKE parameters', () => {
    // `folder\name` is pre-escaped by the schema compiler to `folder\\name`;
    // the driver must not escape those backslashes again.
    const sql = driver.prepareQueryWithParams(
      `SELECT * FROM orders WHERE LOWER(name) LIKE CONCAT('%', LOWER(?), '%') ESCAPE '\\'`,
      ['folder\\\\name'],
    );

    expect(sql).toBe(
      `SELECT * FROM orders WHERE LOWER(name) LIKE CONCAT('%', LOWER('folder\\\\name'), '%') ESCAPE '\\'`
    );
  });

  it('doubles quotes so a LIKE value cannot break out of the literal', () => {
    const sql = driver.prepareQueryWithParams(
      `SELECT * FROM orders WHERE LOWER(name) LIKE CONCAT('%', LOWER(?), '%') ESCAPE '\\'`,
      [`o'reilly'); DROP TABLE orders; --`],
    );

    expect(sql).toBe(
      `SELECT * FROM orders WHERE LOWER(name) LIKE CONCAT('%', LOWER('o''reilly''); DROP TABLE orders; --'), '%') ESCAPE '\\'`
    );
  });

  it('keeps the literal closed for a backslash-then-quote payload', () => {
    // In a backslash-escaping dialect `\'` would smuggle a quote; in standard
    // SQL the backslash is plain data and only the quote is doubled.
    const sql = driver.prepareQueryWithParams(
      'SELECT * FROM orders WHERE name = ?',
      [`foo\\' OR 1=1 --`],
    );

    expect(sql).toBe(`SELECT * FROM orders WHERE name = 'foo\\'' OR 1=1 --'`);
  });

  it('keeps a literal percent sign in an equality parameter verbatim', () => {
    // `%` is only special inside a LIKE pattern; as plain data it must not be
    // escaped or mangled by the driver.
    const sql = driver.prepareQueryWithParams(
      'SELECT * FROM orders WHERE discount_label = ?',
      ['100% cotton'],
    );

    expect(sql).toBe(`SELECT * FROM orders WHERE discount_label = '100% cotton'`);
  });

  it('passes an unescaped percent sign through a LIKE parameter untouched', () => {
    // Escaping wildcards is the schema compiler's job (`50\%`); when a raw `%`
    // reaches the driver it must stay a wildcard, not get double-escaped.
    const sql = driver.prepareQueryWithParams(
      `SELECT * FROM orders WHERE LOWER(name) LIKE CONCAT('%', LOWER(?), '%') ESCAPE '\\'`,
      ['50%'],
    );

    expect(sql).toBe(
      `SELECT * FROM orders WHERE LOWER(name) LIKE CONCAT('%', LOWER('50%'), '%') ESCAPE '\\'`
    );
  });

  it('escapes quotes in a value that also contains percent signs', () => {
    const sql = driver.prepareQueryWithParams(
      `SELECT * FROM orders WHERE LOWER(name) LIKE CONCAT('%', LOWER(?), '%') ESCAPE '\\'`,
      [`50%' OR 1=1 --`],
    );

    expect(sql).toBe(
      `SELECT * FROM orders WHERE LOWER(name) LIKE CONCAT('%', LOWER('50%'' OR 1=1 --'), '%') ESCAPE '\\'`
    );
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
