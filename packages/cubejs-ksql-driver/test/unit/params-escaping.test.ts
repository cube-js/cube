/* eslint-disable quotes */
import { KsqlDriver } from '../../src/KsqlDriver';

class TestKsqlDriver extends KsqlDriver {
  public override prepareQueryWithParams(query: string, values?: unknown[]): string {
    return super.prepareQueryWithParams(query, values);
  }
}

// ksqlDB's grammar descends from Presto's: a quote inside a string literal is
// escaped by doubling it and a backslash is plain data.
describe('KsqlDriver SQL parameter escaping', () => {
  let driver: TestKsqlDriver;

  beforeAll(() => {
    driver = new TestKsqlDriver({ url: 'http://localhost:8088' });
  });

  it('doubles quotes so a value cannot break out of the literal', () => {
    const sql = driver.prepareQueryWithParams(
      'CREATE TABLE t AS SELECT * FROM s WHERE status = ?',
      [`a' OR 1=1 --`],
    );

    expect(sql).toBe(
      `CREATE TABLE t AS SELECT * FROM s WHERE status = 'a'' OR 1=1 --'`
    );
  });

  it('keeps the literal closed for a value ending in a backslash', () => {
    const sql = driver.prepareQueryWithParams(
      'CREATE TABLE t AS SELECT * FROM s WHERE name = ? AND status = ?',
      ['payload\\', 'new'],
    );

    expect(sql).toBe(
      `CREATE TABLE t AS SELECT * FROM s WHERE name = 'payload\\' AND status = 'new'`
    );
  });

  it('keeps the literal closed for a backslash-then-quote payload', () => {
    const sql = driver.prepareQueryWithParams(
      'CREATE TABLE t AS SELECT * FROM s WHERE name = ?',
      [`foo\\' OR 1=1 --`],
    );

    expect(sql).toBe(
      `CREATE TABLE t AS SELECT * FROM s WHERE name = 'foo\\'' OR 1=1 --'`
    );
  });

  it('does not double literal backslashes', () => {
    const sql = driver.prepareQueryWithParams(
      'CREATE TABLE t AS SELECT * FROM s WHERE path = ?',
      ['folder\\\\name'],
    );

    expect(sql).toBe(
      `CREATE TABLE t AS SELECT * FROM s WHERE path = 'folder\\\\name'`
    );
  });

  it('escapes every element of an array parameter', () => {
    const sql = driver.prepareQueryWithParams(
      'CREATE TABLE t AS SELECT * FROM s WHERE status IN (?)',
      [[`it's`, 'b']],
    );

    expect(sql).toBe(
      `CREATE TABLE t AS SELECT * FROM s WHERE status IN ('it''s', 'b')`
    );
  });

  it('substitutes multiple placeholders in order', () => {
    const sql = driver.prepareQueryWithParams(
      'CREATE TABLE t AS SELECT * FROM s WHERE status = ? AND amount > ?',
      ['new', 100],
    );

    expect(sql).toBe(
      `CREATE TABLE t AS SELECT * FROM s WHERE status = 'new' AND amount > 100`
    );
  });

  it('leaves a query without parameters untouched', () => {
    expect(driver.prepareQueryWithParams('SHOW VARIABLES', [])).toBe('SHOW VARIABLES');
  });
});
