/* eslint-disable quotes */
import { TrinoDriver } from '../../src/TrinoDriver';

jest.mock('node-fetch', () => ({
  __esModule: true,
  default: jest.fn(),
}));

jest.mock('@cubejs-backend/schema-compiler', () => ({
  PrestodbQuery: class {},
  TrinoQuery: class {},
}));

class TestTrinoDriver extends TrinoDriver {
  public override prepareQueryWithParams(query: string, values: unknown[]) {
    return super.prepareQueryWithParams(query, values);
  }
}

describe('TrinoDriver SQL parameter escaping', () => {
  let driver: TestTrinoDriver;

  beforeAll(() => {
    driver = new TestTrinoDriver({
      host: 'localhost',
      port: '8080',
      catalog: 'test',
      schema: 'default',
      dataSource: 'default',
    });
  });

  afterAll(async () => {
    await driver.release();
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
    const sql = driver.prepareQueryWithParams(
      'SELECT * FROM orders WHERE name = ?',
      [`foo\\' OR 1=1 --`],
    );

    expect(sql).toBe(`SELECT * FROM orders WHERE name = 'foo\\'' OR 1=1 --'`);
  });

  it('keeps a literal percent sign in an equality parameter verbatim', () => {
    const sql = driver.prepareQueryWithParams(
      'SELECT * FROM orders WHERE discount_label = ?',
      ['100% cotton'],
    );

    expect(sql).toBe(`SELECT * FROM orders WHERE discount_label = '100% cotton'`);
  });

  it('passes an unescaped percent sign through a LIKE parameter untouched', () => {
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
