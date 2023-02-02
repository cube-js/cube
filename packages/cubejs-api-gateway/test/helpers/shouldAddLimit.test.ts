import { shouldAddLimit } from '../../src/helpers/shouldAddLimit';

describe('shouldAddLimit', () => {
  test('returns false for CREATE query', () => {
    const sql = 'CREATE TABLE users (id INT, name VARCHAR(255))';
    expect(shouldAddLimit(sql)).toBe(false);
  });

  test('returns false for ALTER query', () => {
    const sql = 'ALTER TABLE users ADD COLUMN email VARCHAR(255)';
    expect(shouldAddLimit(sql)).toBe(false);
  });

  test('returns false for DROP query', () => {
    const sql = 'DROP TABLE users';
    expect(shouldAddLimit(sql)).toBe(false);
  });

  test('returns false for TRUNCATE query', () => {
    const sql = 'TRUNCATE TABLE users';
    expect(shouldAddLimit(sql)).toBe(false);
  });

  test('returns false for INSERT query', () => {
    const sql = 'INSERT INTO users (id, name) VALUES (1, "John")';
    expect(shouldAddLimit(sql)).toBe(false);
  });

  test('returns false for UPDATE query', () => {
    const sql = 'UPDATE users SET name="Jane" WHERE id=1';
    expect(shouldAddLimit(sql)).toBe(false);
  });

  test('returns false for DELETE query', () => {
    const sql = 'DELETE FROM users WHERE id=1';
    expect(shouldAddLimit(sql)).toBe(false);
  });

  test('returns true for SELECT query', () => {
    const sql = 'SELECT * FROM users';
    expect(shouldAddLimit(sql)).toBe(true);
  });

  test('returns true for WITH query', () => {
    const sql = 'WITH cte AS (SELECT id, name FROM users) SELECT * FROM cte';
    expect(shouldAddLimit(sql)).toBe(true);
  });

  test('throw UserError for invalid SQL query', () => {
    const sql = 'INVALID QUERY';
    expect(() => {
      shouldAddLimit(sql);
    }).toThrow('Invalid SQL query');
  });
});
