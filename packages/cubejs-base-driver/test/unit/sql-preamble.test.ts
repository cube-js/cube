import {
  applySqlPreambleStatements,
  isAlreadyAppliedPreambleError,
  joinSqlPreamble,
  normalizeSqlPreamble,
  prependSqlPreamble,
  resolveSqlPreamble,
  splitSqlPreamble,
  trySplitSqlPreamble,
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

  // Whether the backslash escapes the closing quote decides where the literal
  // ends: Postgres, DuckDB and Snowflake say no (standard_conforming_strings),
  // MySQL and BigQuery say yes. Reading it either way would tear a statement on
  // the other, so the blob goes to the engine whole.
  test('does not pick a dialect for a trailing backslash before a quote', () => {
    expect(splitSqlPreamble('SET a = \'C:\\\'; SET b = 2'))
      .toEqual(['SET a = \'C:\\\'; SET b = 2']);
  });

  // A backslash the dialects read the same way still splits normally.
  test('a backslash away from the closing quote does not block the split', () => {
    expect(splitSqlPreamble('SET a = \'C:\\path\'; SET b = 2'))
      .toEqual(['SET a = \'C:\\path\'', 'SET b = 2']);
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

  // `#` is MySQL's line comment but a Postgres operator (bitwise XOR), so a `;`
  // after it is inside a comment on one and a separator on the other.
  test('does not pick a dialect for a semicolon after a #', () => {
    expect(splitSqlPreamble('SET a = 1 # one; two\n; SET b = 2'))
      .toEqual(['SET a = 1 # one; two\n; SET b = 2']);
  });

  // With no `;` in the disputed span the dialects agree, so the split stands.
  test('a # with no semicolon after it does not block the split', () => {
    expect(splitSqlPreamble('SET a = 1 # note\n; SET b = 2'))
      .toEqual(['SET a = 1 # note', 'SET b = 2']);
  });

  // Postgres, DuckDB and Snowflake all nest block comments, so the first `*/`
  // does not necessarily end one.
  test('honours nested block comments', () => {
    expect(splitSqlPreamble('/* outer /* inner */ SET a = 1; */ SET b = 2'))
      .toEqual(['/* outer /* inner */ SET a = 1; */ SET b = 2']);
  });

  // `$$` is genuinely ambiguous — a quoted-body opener in Postgres, an operator
  // in MySQL — so it is passed through whole rather than split on a guess.
  test('does not split a blob containing an ambiguous $$', () => {
    expect(splitSqlPreamble('SELECT a$$b; SET c = 1'))
      .toEqual(['SELECT a$$b; SET c = 1']);
  });

  test('leaves a $ inside an identifier alone', () => {
    expect(splitSqlPreamble('SET my$var = 1; SET b = 2'))
      .toEqual(['SET my$var = 1', 'SET b = 2']);
  });

  // Splitting on a guess would hand the engine a fragment of the user's SQL, so
  // anything unparseable is passed through whole for the engine to reject.
  describe('unparseable input is not split', () => {
    test('an unterminated literal', () => {
      expect(splitSqlPreamble('SET a = \'unterminated; SET b = 2'))
        .toEqual(['SET a = \'unterminated; SET b = 2']);
    });

    test('an unterminated block comment', () => {
      expect(splitSqlPreamble('SET a = 1; /* never closed'))
        .toEqual(['SET a = 1; /* never closed']);
    });

    test('an unterminated dollar-quoted body', () => {
      expect(splitSqlPreamble('CREATE FUNCTION f() AS $$ SELECT 1; SET b = 2'))
        .toEqual(['CREATE FUNCTION f() AS $$ SELECT 1; SET b = 2']);
    });

    // The two readings of a backslash-escaped quote disagree about where the
    // literal ends, which is real ambiguity rather than a failure to parse.
    test('a dialect-dependent quote escape', () => {
      expect(splitSqlPreamble('SET a = \'a\\\'; SET b = \'c\''))
        .toEqual(['SET a = \'a\\\'; SET b = \'c\'']);
      expect(splitSqlPreamble('SET a = \'it\\\'s; fine\'; SET b = 2'))
        .toEqual(['SET a = \'it\\\'s; fine\'; SET b = 2']);
    });

    test('a dialect-dependent # before a separator', () => {
      expect(splitSqlPreamble('SET a = 5 # 3; SET b = 1'))
        .toEqual(['SET a = 5 # 3; SET b = 1']);
    });
  });

  // A segment holding only a comment has no executable token. Snowflake rejects
  // an empty statement, and BigQuery's script-safety guard would refuse a
  // legitimate UDF preamble that simply ends with a comment.
  describe('a segment with no executable token is not a statement', () => {
    test('a trailing line comment', () => {
      expect(splitSqlPreamble('SET a = 1;\n-- note')).toEqual(['SET a = 1']);
    });

    test('a trailing block comment', () => {
      expect(splitSqlPreamble('SET a = 1;\n/* note */')).toEqual(['SET a = 1']);
    });

    test('a comment between two statements', () => {
      expect(splitSqlPreamble('SET a = 1;\n-- note\n;\nSET b = 2'))
        .toEqual(['SET a = 1', 'SET b = 2']);
    });

    test('a comment-only preamble yields no statements', () => {
      expect(splitSqlPreamble('-- nothing to do')).toEqual([]);
      expect(splitSqlPreamble('/* nothing to do */')).toEqual([]);
    });

    // The comment still travels with the statement it documents.
    test('a leading comment stays part of its statement', () => {
      expect(splitSqlPreamble('-- why\nSET a = 1')).toEqual(['-- why\nSET a = 1']);
    });
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

// The flag exists so a caller deciding whether a blob is *shaped* a certain way
// cannot mistake "one unparsed blob" for "one statement".
describe('trySplitSqlPreamble', () => {
  test('reports a confident split', () => {
    expect(trySplitSqlPreamble('SET a = 1; SET b = 2'))
      .toEqual({ statements: ['SET a = 1', 'SET b = 2'], ambiguous: false });
  });

  test('reports ambiguity and returns the blob whole', () => {
    expect(trySplitSqlPreamble('SET a = 1; /* never closed'))
      .toEqual({ statements: ['SET a = 1; /* never closed'], ambiguous: true });
  });

  test('an empty preamble is not ambiguous', () => {
    expect(trySplitSqlPreamble(undefined)).toEqual({ statements: [], ambiguous: false });
    expect(trySplitSqlPreamble('  ')).toEqual({ statements: [], ambiguous: false });
  });

  test('splitSqlPreamble returns exactly its statements', () => {
    for (const preamble of ['SET a = 1; SET b = 2', 'SET a = 1; /* never closed', '  ']) {
      expect(splitSqlPreamble(preamble)).toEqual(trySplitSqlPreamble(preamble).statements);
    }
  });
});

describe('applySqlPreambleStatements', () => {
  // A caller that has already separated the preamble from other statements must
  // not have to re-join and re-parse it — re-parsing could go ambiguous and
  // collapse the list back into one blob.
  test('accepts an already-split statement list', async () => {
    const executed: string[] = [];

    await applySqlPreambleStatements(['SET a = 1', 'SET b = 2'], async statement => {
      executed.push(statement);
    });

    expect(executed).toEqual(['SET a = 1', 'SET b = 2']);
  });

  test('a statement list is executed verbatim, not re-split', async () => {
    const executed: string[] = [];

    // Re-joining and re-splitting this would go ambiguous and run it as one.
    await applySqlPreambleStatements(['SET a = \'a\\\'', 'SET b = \'c\''], async statement => {
      executed.push(statement);
    });

    expect(executed).toEqual(['SET a = \'a\\\'', 'SET b = \'c\'']);
  });

  test('tolerates an already-applied statement from a list too', async () => {
    const executed: string[] = [];

    await applySqlPreambleStatements(['CREATE MACRO m(x) AS x', 'SET a = 1'], async statement => {
      executed.push(statement);

      if (statement.startsWith('CREATE')) {
        throw new Error('Catalog Error: Function with name "m" already exists!');
      }
    });

    expect(executed).toEqual(['CREATE MACRO m(x) AS x', 'SET a = 1']);
  });

  test('runs each statement in order', async () => {
    const executed: string[] = [];

    await applySqlPreambleStatements('SET a = 1; SET b = 2', async statement => {
      executed.push(statement);
    });

    expect(executed).toEqual(['SET a = 1', 'SET b = 2']);
  });

  test('runs nothing when no preamble is set', async () => {
    const execute = jest.fn();

    await applySqlPreambleStatements(undefined, execute);
    await applySqlPreambleStatements('  ', execute);

    expect(execute).not.toHaveBeenCalled();
  });

  // Pooled drivers re-run the preamble on each acquired connection, so a
  // CREATE statement that already took effect must not fail the query.
  test('skips a statement already applied on this connection', async () => {
    const executed: string[] = [];

    await applySqlPreambleStatements('CREATE TEMP TABLE t (x int); SET a = 1', async statement => {
      executed.push(statement);

      if (statement.startsWith('CREATE')) {
        throw new Error('relation "t" already exists');
      }
    });

    expect(executed).toEqual(['CREATE TEMP TABLE t (x int)', 'SET a = 1']);
  });

  test('still surfaces a genuine error', async () => {
    await expect(applySqlPreambleStatements('THIS IS NOT SQL', async () => {
      throw new Error('syntax error at or near "THIS"');
    })).rejects.toThrow('syntax error');
  });

  test('surfaces a permission error rather than skipping it', async () => {
    await expect(applySqlPreambleStatements('CREATE FUNCTION f()', async () => {
      throw new Error('permission denied for schema public');
    })).rejects.toThrow('permission denied');
  });
});

describe('isAlreadyAppliedPreambleError', () => {
  test.each([
    'relation "t" already exists',
    'Catalog Error: Table with name t already exists!',
    'Function ALREADY EXISTS',
    'Duplicate key name \'idx\'',
    'variable is already defined',
  ])('treats %s as already applied', message => {
    expect(isAlreadyAppliedPreambleError(new Error(message))).toBe(true);
  });

  test.each([
    'syntax error at or near "SELCT"',
    'permission denied for schema public',
    'connection terminated unexpectedly',
    'relation "t" does not exist',
  ])('treats %s as a real failure', message => {
    expect(isAlreadyAppliedPreambleError(new Error(message))).toBe(false);
  });

  test('tolerates a non-error value', () => {
    expect(isAlreadyAppliedPreambleError(undefined)).toBe(false);
    expect(isAlreadyAppliedPreambleError('already exists')).toBe(false);
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
  test('prefers the config option over the environment', () => {
    expect(resolveSqlPreamble({ sqlPreamble: 'SET config = 1' }, 'SET env = 1'))
      .toEqual('SET config = 1');
  });

  test('falls back to the environment when the option is unset', () => {
    expect(resolveSqlPreamble({}, 'SET env = 1')).toEqual('SET env = 1');
    expect(resolveSqlPreamble({ sqlPreamble: undefined }, 'SET env = 1')).toEqual('SET env = 1');
  });

  // `sqlPreamble: process.env.MY_PREAMBLE || ''` is easy to template into a
  // config, and must not silently disable CUBEJS_DB_SQL_PREAMBLE.
  test('a blank option falls through to the environment rather than suppressing it', () => {
    expect(resolveSqlPreamble({ sqlPreamble: '' }, 'SET env = 1')).toEqual('SET env = 1');
    expect(resolveSqlPreamble({ sqlPreamble: '   ' }, 'SET env = 1')).toEqual('SET env = 1');
  });

  test('trims whichever value it resolves', () => {
    expect(resolveSqlPreamble({ sqlPreamble: '  SET a = 1  ' })).toEqual('SET a = 1');
    expect(resolveSqlPreamble({}, '  SET a = 1  ')).toEqual('SET a = 1');
  });

  test('returns undefined when neither is configured', () => {
    expect(resolveSqlPreamble({})).toBeUndefined();
    expect(resolveSqlPreamble({ sqlPreamble: '  ' }, '  ')).toBeUndefined();
    expect(resolveSqlPreamble({}, undefined)).toBeUndefined();
  });
});
