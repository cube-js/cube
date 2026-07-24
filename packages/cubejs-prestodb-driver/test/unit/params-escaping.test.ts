/* eslint-disable quotes */
import { PrestoDriver } from '../../src/PrestoDriver';

describe('PrestoDriver SQL parameter escaping', () => {
  let driver: PrestoDriver;

  beforeAll(() => {
    driver = new PrestoDriver({
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
