import {
  shouldAddLimit,
  isQueryAllowed,
} from '../../src/helpers/sqlQueryHandler';

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

describe('isQueryAllowed', () => {
  test('Should return true if the scope is empty', () => {
    const scope = [];
    const query = 'SELECT * FROM test';
    expect(isQueryAllowed(query, scope)).toBe(true);
  });

  test('Should return true if the scope does not contain sql-runner-permissions', () => {
    const scope = ['test-permissions:create,select,update,delete'];
    const query = 'SELECT * FROM test';
    expect(isQueryAllowed(query, scope)).toBe(true);
  });

  test('Should return false if the sql-runner-permissions is empty', () => {
    const scope = ['sql-runner-permissions:'];
    const query = 'SELECT * FROM test';
    expect(isQueryAllowed(query, scope)).toBe(false);
  });

  test('Should return true if the statement type is included in the sql-runner-permissions', () => {
    const scope = ['sql-runner-permissions:select'];
    const query = 'SELECT * FROM test';
    expect(isQueryAllowed(query, scope)).toBe(true);
  });

  test('Should return false if the statement type is not included in the sql-runner-permissions', () => {
    const scope = ['sql-runner-permissions:create,update,delete'];
    const query = 'SELECT id, count FROM test';
    expect(isQueryAllowed(query, scope)).toBe(false);
  });

  test('Should return true if the statement type is included in the sql-runner-permissions for CREATE statement', () => {
    const scope = ['sql-runner-permissions:create'];
    const query = 'CREATE TABLE test (id BIGINT)';
    expect(isQueryAllowed(query, scope)).toBe(true);
  });

  test('Should return false if the statement type is not included in the sql-runner-permissions for CREATE statement', () => {
    const scope = ['sql-runner-permissions:select,update,delete'];
    const query = 'CREATE TABLE test (id BIGINT)';
    expect(isQueryAllowed(query, scope)).toBe(false);
  });

  test('Should return true if the statement type is included in the sql-runner-permissions for ALTER statement', () => {
    const scope = ['sql-runner-permissions:alter'];
    const query = 'ALTER TABLE test ADD COLUMN test_num INT';
    expect(isQueryAllowed(query, scope)).toBe(true);
  });

  test('Should return true if the statement type is included in the sql-runner-permissions for EXPLAIN statement', () => {
    const scope = ['sql-runner-permissions:create,explain'];
    const query = 'EXPLAIN SELECT id FROM test';
    expect(isQueryAllowed(query, scope)).toBe(true);
  });

  test('Should throw an error if wrong statement', () => {
    const scope = ['sql-runner-permissions:create'];
    const query = 'ERR_SELECT id FROM test';
    expect(() => isQueryAllowed(query, scope)).toThrow('Invalid SQL query');
  });
});
