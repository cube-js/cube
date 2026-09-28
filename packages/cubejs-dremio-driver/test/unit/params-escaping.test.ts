/* eslint-disable quotes */
const { applyParams } = require('../../../driver/DremioDriver');

// Dremio is a Calcite-based, standard-SQL engine: a quote inside a string
// literal is escaped by doubling it and a backslash is plain data.
describe('DremioDriver SQL parameter escaping', () => {
  it('preserves LIKE escape sequences emitted by the schema compiler', () => {
    const sql = applyParams(
      `SELECT * FROM orders WHERE LOWER(name) LIKE '%' || LOWER(?) || '%' ESCAPE '\\'`,
      ['new\\_order\\%'],
    );

    expect(sql).toBe(
      `SELECT * FROM orders WHERE LOWER(name) LIKE '%' || LOWER('new\\_order\\%') || '%' ESCAPE '\\'`
    );
  });

  it('does not double literal backslashes in LIKE parameters', () => {
    const sql = applyParams(
      `SELECT * FROM orders WHERE LOWER(name) LIKE '%' || LOWER(?) || '%' ESCAPE '\\'`,
      ['folder\\\\name'],
    );

    expect(sql).toBe(
      `SELECT * FROM orders WHERE LOWER(name) LIKE '%' || LOWER('folder\\\\name') || '%' ESCAPE '\\'`
    );
  });

  it('doubles quotes so a value cannot break out of the literal', () => {
    const sql = applyParams(
      'SELECT * FROM orders WHERE name = ?',
      [`o'reilly'); DROP TABLE orders; --`],
    );

    expect(sql).toBe(
      `SELECT * FROM orders WHERE name = 'o''reilly''); DROP TABLE orders; --'`
    );
  });

  it('keeps the literal closed for a backslash-then-quote payload', () => {
    // In a backslash-escaping dialect `\'` would smuggle a quote through; on
    // Dremio the backslash is data and only the quote needs doubling.
    const sql = applyParams(
      'SELECT * FROM orders WHERE name = ?',
      [`foo\\' OR 1=1 --`],
    );

    expect(sql).toBe(`SELECT * FROM orders WHERE name = 'foo\\'' OR 1=1 --'`);
  });

  it('keeps the literal closed for a value ending in a backslash', () => {
    const sql = applyParams(
      'SELECT * FROM orders WHERE name = ? AND status = ?',
      ['payload\\', 'new'],
    );

    expect(sql).toBe(`SELECT * FROM orders WHERE name = 'payload\\' AND status = 'new'`);
  });

  it('keeps a literal percent sign in an equality parameter verbatim', () => {
    const sql = applyParams(
      'SELECT * FROM orders WHERE discount_label = ?',
      ['100% cotton'],
    );

    expect(sql).toBe(`SELECT * FROM orders WHERE discount_label = '100% cotton'`);
  });

  it('escapes every element of an array parameter', () => {
    const sql = applyParams(
      'SELECT * FROM orders WHERE status IN (?)',
      [[`it's`, 'b']],
    );

    expect(sql).toBe(`SELECT * FROM orders WHERE status IN ('it''s', 'b')`);
  });

  it('substitutes multiple placeholders in order', () => {
    const sql = applyParams(
      `SELECT * FROM orders WHERE LOWER(name) LIKE '%' || LOWER(?) || '%' ESCAPE '\\' AND status = ? AND amount > ?`,
      ['pending\\_review', 'new', 100],
    );

    expect(sql).toBe(
      `SELECT * FROM orders WHERE LOWER(name) LIKE '%' || LOWER('pending\\_review') || '%' ESCAPE '\\' AND status = 'new' AND amount > 100`
    );
  });
});
