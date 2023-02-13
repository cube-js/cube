import {
  getSQLRunnerQueryType,
  SQLQueryType,
  isQueryAllowed,
} from '../../src/helpers/sqlQueryHandler';

describe('getSQLRunnerQueryType', () => {
  test('should return the correct SQLRunnerQueryType for a SELECT query', () => {
    const query = 'SELECT * FROM users';
    const expected = {
      type: SQLQueryType.Select,
      regex: /^(select)\b/i,
      shouldAddLimit: true,
    };

    expect(getSQLRunnerQueryType(query)).toEqual(expected);
  });

  test('should return the correct SQLRunnerQueryType for a WITH query', () => {
    const query =
      'WITH users_cte AS (SELECT * FROM users) SELECT * FROM users_cte';
    const expected = {
      type: SQLQueryType.With,
      regex: /^(with)\b/i,
      shouldAddLimit: true,
    };

    expect(getSQLRunnerQueryType(query)).toEqual(expected);
  });

  test('should return the correct SQLRunnerQueryType for a CREATE query', () => {
    const query = 'CREATE TABLE users (id INT, name TEXT)';
    const expected = {
      type: SQLQueryType.Create,
      regex: /^(create)\b/i,
      shouldAddLimit: false,
    };

    expect(getSQLRunnerQueryType(query)).toEqual(expected);
  });

  test('should return the correct SQLRunnerQueryType for an ALTER query', () => {
    const query = 'ALTER TABLE users ADD COLUMN email TEXT';
    const expected = {
      type: SQLQueryType.Alter,
      regex: /^(alter)\b/i,
      shouldAddLimit: false,
    };

    expect(getSQLRunnerQueryType(query)).toEqual(expected);
  });

  test('should return the correct SQLRunnerQueryType for a DROP query', () => {
    const query = 'DROP TABLE users';
    const expected = {
      type: SQLQueryType.Drop,
      regex: /^(drop)\b/i,
      shouldAddLimit: false,
    };

    expect(getSQLRunnerQueryType(query)).toEqual(expected);
  });

  test('should return the correct SQLRunnerQueryType for a TRUNCATE query', () => {
    const query = 'TRUNCATE TABLE users';
    const expected = {
      type: SQLQueryType.Truncate,
      regex: /^(truncate)\b/i,
      shouldAddLimit: false,
    };

    expect(getSQLRunnerQueryType(query)).toEqual(expected);
  });

  test('should return the correct SQLRunnerQueryType for an INSERT query', () => {
    const query = 'INSERT INTO users (id, name) VALUES (1, \'John\')';
    const expected = {
      type: SQLQueryType.Insert,
      regex: /^(insert)\b/i,
      shouldAddLimit: false,
    };

    expect(getSQLRunnerQueryType(query)).toEqual(expected);
  });

  test('should return the correct SQLRunnerQueryType for a DELETE query', () => {
    const query = 'DELETE FROM users WHERE id = 1';
    const expected = {
      type: SQLQueryType.Delete,
      regex: /^(delete)\b/i,
      shouldAddLimit: false,
    };

    expect(getSQLRunnerQueryType(query)).toEqual(expected);
  });

  test('should return the correct SQLRunnerQueryType for an EXPLAIN query', () => {
    const query = 'EXPLAIN SELECT * FROM users';
    const expected = {
      type: SQLQueryType.Explain,
      regex: /^(explain)\b/i,
      shouldAddLimit: false,
    };

    expect(getSQLRunnerQueryType(query)).toEqual(expected);
  });

  test('should return null for not supported query', () => {
    const query = 'PSELECT FROM test';

    expect(getSQLRunnerQueryType(query)).toBeNull();
  });

  test('should be case-insensitive', () => {
    const query = 'sElEcT * FrOm UsErS';
    const expected = {
      type: SQLQueryType.Select,
      regex: /^(select)\b/i,
      shouldAddLimit: true,
    };

    expect(getSQLRunnerQueryType(query)).toEqual(expected);
  });
});

describe('isQueryAllowed', () => {
  test('should return true if the scope does not include sql-runner-permissions', () => {
    const scope = ['read:datasets', 'write:datasets'];
    const queryType = getSQLRunnerQueryType('SELECT * FROM table')!;

    expect(isQueryAllowed(queryType, scope)).toBe(true);
  });

  test('should return false if the scope includes an empty sql-runner-permissions', () => {
    const scope = [
      'read:datasets',
      'write:datasets',
      'sql-runner-permissions:',
    ];
    const queryType = getSQLRunnerQueryType('SELECT * FROM table')!;

    expect(isQueryAllowed(queryType, scope)).toBe(false);
  });

  test('should return true if the scope includes the type of the query', () => {
    const scope = [
      'read:datasets',
      'write:datasets',
      'sql-runner-permissions:select',
    ];
    const queryType = getSQLRunnerQueryType('SELECT * FROM table')!;

    expect(isQueryAllowed(queryType, scope)).toBe(true);
  });

  test('should return false if the scope does not include the type of the query', () => {
    const scope = [
      'read:datasets',
      'write:datasets',
      'sql-runner-permissions:create',
    ];
    const queryType = getSQLRunnerQueryType('SELECT * FROM table')!;

    expect(isQueryAllowed(queryType, scope)).toBe(false);
  });

  test('should return false if the scope does not contain the required permission for the query type', () => {
    const queryType = getSQLRunnerQueryType(
      'CREATE TABLE table_name (col1 INTEGER, col2 VARCHAR(255))'
    )!;
    const scope = ['read:data', 'write:data', 'sql-runner-permissions:select'];

    expect(isQueryAllowed(queryType, scope)).toBe(false);
  });

  test('should return true if the scope contains multiple required permissions for the query type', () => {
    const queryType = getSQLRunnerQueryType('SELECT * FROM table')!;
    const scope = [
      'read:data',
      'write:data',
      'sql-runner-permissions:select,with',
    ];

    const result = isQueryAllowed(queryType, scope);

    expect(result).toBe(true);
  });
});
