import {
  joinSqlPreamble,
  normalizeSqlPreamble,
  prependSqlPreamble,
  resolveSqlPreamble,
  splitSqlPreamble,
} from '../../src/sql-preamble';

describe('normalizeSqlPreamble', () => {
  test('trims and passes through a real value', () => {
    expect(normalizeSqlPreamble('  SET x = 1  ')).toEqual('SET x = 1');
  });

  test('treats blank and missing values as not configured', () => {
    expect(normalizeSqlPreamble('')).toBeUndefined();
    expect(normalizeSqlPreamble('   \n\t ')).toBeUndefined();
    expect(normalizeSqlPreamble(undefined)).toBeUndefined();
    expect(normalizeSqlPreamble(null)).toBeUndefined();
  });
});

describe('joinSqlPreamble', () => {
  test('joins the legacy array shape into one blob', () => {
    expect(joinSqlPreamble(['SET a = 1', 'SET b = 2'])).toEqual('SET a = 1;\nSET b = 2');
  });

  test('does not double-terminate already-terminated statements', () => {
    expect(joinSqlPreamble(['SET a = 1;', 'SET b = 2;'])).toEqual('SET a = 1;\nSET b = 2');
  });

  test('drops blank entries rather than emitting empty statements', () => {
    expect(joinSqlPreamble(['SET a = 1', '', '   '])).toEqual('SET a = 1');
    expect(joinSqlPreamble([])).toBeUndefined();
    expect(joinSqlPreamble(['', ' '])).toBeUndefined();
  });

  test('passes a plain string through', () => {
    expect(joinSqlPreamble('SET a = 1')).toEqual('SET a = 1');
  });
});

describe('splitSqlPreamble', () => {
  test('splits plain statements and drops the trailing separator', () => {
    expect(splitSqlPreamble('SET a = 1; SET b = 2;')).toEqual(['SET a = 1', 'SET b = 2']);
  });

  test('returns a single statement unsplit', () => {
    expect(splitSqlPreamble('SET a = 1')).toEqual(['SET a = 1']);
  });

  test('returns nothing for a blank preamble', () => {
    expect(splitSqlPreamble(undefined)).toEqual([]);
    expect(splitSqlPreamble('  ')).toEqual([]);
    expect(splitSqlPreamble(';;')).toEqual([]);
  });

  // The feature's main use case is defining a UDF, whose body is full of
  // semicolons. A naive split(';') would tear these apart.
  test('does not split on a semicolon inside a single-quoted literal', () => {
    expect(splitSqlPreamble('SET note = \'a;b\'; SET c = 2'))
      .toEqual(['SET note = \'a;b\'', 'SET c = 2']);
  });

  test('does not split on a semicolon inside a double-quoted identifier', () => {
    expect(splitSqlPreamble('SET "we;ird" = 1; SET c = 2'))
      .toEqual(['SET "we;ird" = 1', 'SET c = 2']);
  });

  test('does not split on a semicolon inside a backtick identifier', () => {
    expect(splitSqlPreamble('SET `we;ird` = 1; SET c = 2'))
      .toEqual(['SET `we;ird` = 1', 'SET c = 2']);
  });

  test('honours a doubled quote as an escaped quote, not a terminator', () => {
    expect(splitSqlPreamble('SET a = \'it\'\'s; fine\'; SET b = 2'))
      .toEqual(['SET a = \'it\'\'s; fine\'', 'SET b = 2']);
  });

  test('honours a backslash-escaped quote', () => {
    expect(splitSqlPreamble('SET a = \'x\\\'; y\'; SET b = 2'))
      .toEqual(['SET a = \'x\\\'; y\'', 'SET b = 2']);
  });

  test('keeps a dollar-quoted function body intact', () => {
    const preamble = 'CREATE FUNCTION f() RETURNS int AS $$ BEGIN; RETURN 1; END; $$ LANGUAGE plpgsql; SET a = 1';

    expect(splitSqlPreamble(preamble)).toEqual([
      'CREATE FUNCTION f() RETURNS int AS $$ BEGIN; RETURN 1; END; $$ LANGUAGE plpgsql',
      'SET a = 1',
    ]);
  });

  test('keeps a tagged dollar-quoted body intact', () => {
    const preamble = 'CREATE FUNCTION f() AS $body$ SELECT 1; $body$; SET a = 1';

    expect(splitSqlPreamble(preamble)).toEqual([
      'CREATE FUNCTION f() AS $body$ SELECT 1; $body$',
      'SET a = 1',
    ]);
  });

  test('does not split on a semicolon inside a line comment', () => {
    expect(splitSqlPreamble('SET a = 1 -- one; two\n; SET b = 2'))
      .toEqual(['SET a = 1 -- one; two', 'SET b = 2']);
  });

  test('does not split on a semicolon inside a block comment', () => {
    expect(splitSqlPreamble('SET a = 1 /* one; two */; SET b = 2'))
      .toEqual(['SET a = 1 /* one; two */', 'SET b = 2']);
  });

  test('tolerates an unterminated literal without dropping the statement', () => {
    expect(splitSqlPreamble('SET a = \'unterminated')).toEqual(['SET a = \'unterminated']);
  });

  test('splits a BigQuery temp UDF preamble at the statement boundary only', () => {
    const preamble = `CREATE TEMP FUNCTION median(arr ARRAY<FLOAT64>) RETURNS FLOAT64 AS ((
      SELECT AVG(v) FROM (SELECT v FROM UNNEST(arr) v ORDER BY v)
    ));
    SET @@dataset_id = 'analytics';`;

    const statements = splitSqlPreamble(preamble);

    expect(statements).toHaveLength(2);
    expect(statements[0]).toContain('CREATE TEMP FUNCTION median');
    expect(statements[1]).toEqual('SET @@dataset_id = \'analytics\'');
  });
});

describe('prependSqlPreamble', () => {
  test('prepends and terminates the preamble', () => {
    expect(prependSqlPreamble('SELECT 1', 'SET a = 1')).toEqual('SET a = 1;\nSELECT 1');
  });

  test('does not double-terminate an already-terminated preamble', () => {
    expect(prependSqlPreamble('SELECT 1', 'SET a = 1;')).toEqual('SET a = 1;\nSELECT 1');
    expect(prependSqlPreamble('SELECT 1', 'SET a = 1;  ')).toEqual('SET a = 1;\nSELECT 1');
  });

  test('returns the query untouched when no preamble is set', () => {
    expect(prependSqlPreamble('SELECT 1', undefined)).toEqual('SELECT 1');
    expect(prependSqlPreamble('SELECT 1', '  ')).toEqual('SELECT 1');
  });
});

describe('resolveSqlPreamble', () => {
  test('resolves the new option', () => {
    expect(resolveSqlPreamble({ sqlPreamble: 'SET a = 1' })).toEqual('SET a = 1');
  });

  test('resolves the deprecated initSql alias and warns', () => {
    const logger = jest.fn();

    expect(resolveSqlPreamble({ initSql: 'SET a = 1' }, logger)).toEqual('SET a = 1');
    expect(logger).toHaveBeenCalledTimes(1);
    expect(logger.mock.calls[0][1].warning).toContain('initSql');
    expect(logger.mock.calls[0][1].warning).toContain('sqlPreamble');
  });

  test('resolves the deprecated prepareConnectionQueries alias, joining the array shape', () => {
    const logger = jest.fn();

    expect(resolveSqlPreamble({ prepareConnectionQueries: ['SET a = 1', 'SET b = 2'] }, logger))
      .toEqual('SET a = 1;\nSET b = 2');
    expect(logger.mock.calls[0][1].warning).toContain('prepareConnectionQueries');
  });

  test('accepts a string for the legacy array-shaped alias', () => {
    expect(resolveSqlPreamble({ prepareConnectionQueries: 'SET a = 1' })).toEqual('SET a = 1');
  });

  // Precedence is fixed rather than merged: concatenating would run statements
  // the user never asked to combine, and a leftover legacy value must not
  // override the name they migrated to.
  test('prefers the new option over both aliases, without warning', () => {
    const logger = jest.fn();

    expect(resolveSqlPreamble({
      sqlPreamble: 'SET new = 1',
      initSql: 'SET old = 1',
      prepareConnectionQueries: ['SET older = 1'],
    }, logger)).toEqual('SET new = 1');
    expect(logger).not.toHaveBeenCalled();
  });

  test('prefers initSql over prepareConnectionQueries when both legacy names are set', () => {
    expect(resolveSqlPreamble({
      initSql: 'SET old = 1',
      prepareConnectionQueries: ['SET older = 1'],
    })).toEqual('SET old = 1');
  });

  test('returns undefined and does not warn when nothing is configured', () => {
    const logger = jest.fn();

    expect(resolveSqlPreamble({}, logger)).toBeUndefined();
    expect(resolveSqlPreamble({ sqlPreamble: '', initSql: '  ' }, logger)).toBeUndefined();
    expect(logger).not.toHaveBeenCalled();
  });

  test('works without a logger', () => {
    expect(resolveSqlPreamble({ initSql: 'SET a = 1' })).toEqual('SET a = 1');
  });
});
