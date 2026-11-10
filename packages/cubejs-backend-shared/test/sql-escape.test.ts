/* eslint-disable quotes */
import {
  escapeIdentifierAnsi,
  escapeIdentifierBackslash,
  escapeIdentifierMySql,
  escapeIdentifierSparkSql,
  escapeStringLiteral,
  format,
  formatAnsi,
  formatClickHouse,
  formatMySql,
  formatSparkSql,
} from '../src/sql-escape';

const injectionPayloads: Array<[string, string]> = [
  ['boolean tautology', "' OR 1=1 --"],
  ['stacked statement', "'; DROP TABLE users; --"],
  ['UNION query', "' UNION SELECT password FROM users --"],
  ['block-comment obfuscation', "'/**/OR/**/TRUE/*"],
  ['newline after a comment', "'--\nOR 1=1 --"],
  ['backslash-prefixed quote', "\\' OR 1=1 --"],
  ['NUL-prefixed quote', "\0' OR 1=1 --"],
];

/**
 * Parses the subset of ANSI string-literal syntax emitted by SqlEscaper.
 * Encountering an unmatched quote means the escaper allowed an early close.
 */
function decodeAnsiLiteral(literal: string): string {
  if (literal.length < 2 || literal[0] !== "'" || literal[literal.length - 1] !== "'") {
    throw new Error('Not a quoted ANSI string literal');
  }

  const body = literal.slice(1, -1);
  let decoded = '';

  for (let i = 0; i < body.length; i++) {
    if (body[i] === "'") {
      if (body[i + 1] !== "'") {
        throw new Error('Unescaped quote in ANSI string literal');
      }
      decoded += "'";
      i++;
    } else {
      decoded += body[i];
    }
  }
  return decoded;
}

/**
 * Parses the subset of MySQL string-literal syntax emitted by SqlEscaper.
 * Both quotes and backslashes must occur in escaped pairs in the literal body.
 */
function decodeMySqlLiteral(literal: string): string {
  if (literal.length < 2 || literal[0] !== "'" || literal[literal.length - 1] !== "'") {
    throw new Error('Not a quoted MySQL string literal');
  }

  const body = literal.slice(1, -1);
  let decoded = '';

  for (let i = 0; i < body.length; i++) {
    const char = body[i];
    if (char === "'" || char === '\\') {
      if (body[i + 1] !== char) {
        throw new Error(`Unescaped ${char === "'" ? 'quote' : 'backslash'} in MySQL string literal`);
      }
      decoded += char;
      i++;
    } else {
      decoded += char;
    }
  }
  return decoded;
}

/**
 * Parses the subset of Spark SQL / Hive string-literal syntax emitted by
 * SqlEscaper. A doubled quote is not an escape there, so any quote in the body
 * must be backslash-prefixed — an unescaped one means the escaper let the
 * literal close early (and Spark would concatenate whatever follows).
 */
function decodeSparkLiteral(literal: string): string {
  if (literal.length < 2 || literal[0] !== "'" || literal[literal.length - 1] !== "'") {
    throw new Error('Not a quoted Spark string literal');
  }

  const body = literal.slice(1, -1);
  let decoded = '';

  for (let i = 0; i < body.length; i++) {
    const char = body[i];
    if (char === "'") {
      throw new Error('Unescaped quote in Spark string literal');
    }
    if (char === '\\') {
      const next = body[i + 1];
      if (next !== '\\' && next !== "'") {
        throw new Error('Unescaped backslash in Spark string literal');
      }
      decoded += next;
      i++;
    } else {
      decoded += char;
    }
  }
  return decoded;
}

function stringsUpToLength(alphabet: string[], maxLength: number): string[] {
  const values = [''];
  let current = [''];

  for (let length = 1; length <= maxLength; length++) {
    current = current.flatMap(prefix => alphabet.map(char => prefix + char));
    values.push(...current);
  }

  return values;
}

describe('sql-escape', () => {
  const presto = {
    escapeString: (value: string) => formatAnsi('?', [value]),
    escapeIdentifier: (value: string) => formatAnsi('??', [value]),
    escapeValue: (value: unknown) => formatAnsi('?', [value]),
    format: formatAnsi,
  };
  const mysql = {
    escapeString: (value: string) => formatMySql('?', [value]),
    escapeIdentifier: (value: string) => formatMySql('??', [value]),
    escapeValue: (value: unknown) => formatMySql('?', [value]),
    format: formatMySql,
  };
  const spark = {
    escapeString: (value: string) => formatSparkSql('?', [value]),
    escapeIdentifier: (value: string) => formatSparkSql('??', [value]),
    escapeValue: (value: unknown) => formatSparkSql('?', [value]),
    format: formatSparkSql,
  };

  describe('escapeString — ANSI/Presto (double quotes, backslash literal)', () => {
    it('doubles single quotes', () => {
      expect(presto.escapeString("it's")).toBe("'it''s'");
    });

    it('leaves backslashes untouched', () => {
      expect(presto.escapeString('a\\b')).toBe("'a\\b'");
    });

    it('preserves LIKE escape sequences produced by the schema compiler', () => {
      // BaseFilter escapes _ and % as \_ \% for `... LIKE ? ESCAPE '\'`
      expect(presto.escapeString('100\\%\\_raise')).toBe("'100\\%\\_raise'");
    });

    it('neutralizes the classic quote-breakout payload', () => {
      // The bug in PrestoDriver produced: '\' OR 1=1 --'  (injectable).
      const payload = "' OR 1=1 --";
      expect(presto.escapeString(payload)).toBe("''' OR 1=1 --'");
    });

    it('neutralizes a trailing backslash (cannot escape the closing quote)', () => {
      expect(presto.escapeString('\\')).toBe("'\\'");
    });

    it.each(injectionPayloads)('keeps the %s payload inside one literal', (_name, payload) => {
      expect(decodeAnsiLiteral(presto.escapeString(payload))).toBe(payload);
    });

    it('round-trips every short combination of dangerous characters', () => {
      const candidates = stringsUpToLength(["'", '\\', '\0', '\n', '\r', 'a'], 4);

      for (const candidate of candidates) {
        expect(decodeAnsiLiteral(presto.escapeString(candidate))).toBe(candidate);
      }
    });

    it('doubles every quote in consecutive quote runs', () => {
      expect(presto.escapeString("a''b'''c")).toBe("'a''''b''''''c'");
    });

    it('preserves representative Unicode and invisible characters', () => {
      const values = [
        'Café 日本語 👨‍👩‍👧‍👦',
        'cafe\u0301',
        'مرحبا\u00A0بالعالم',
        'left\u200Dright',
      ];

      for (const value of values) {
        expect(decodeAnsiLiteral(presto.escapeString(value))).toBe(value);
      }
    });
  });

  describe('escapeString — MySQL (backslash escapes)', () => {
    it('doubles backslashes and quotes', () => {
      expect(mysql.escapeString('a\\b')).toBe("'a\\\\b'");
      expect(mysql.escapeString("it's")).toBe("'it''s'");
    });

    it('neutralizes a trailing backslash + quote payload', () => {
      // Input: \' — a naive escaper leaves a live quote. Here the backslash is
      // doubled and the quote doubled, so the literal stays closed.
      expect(mysql.escapeString("\\'")).toBe("'\\\\'''");
    });

    it.each(injectionPayloads)('keeps the %s payload inside one literal', (_name, payload) => {
      expect(decodeMySqlLiteral(mysql.escapeString(payload))).toBe(payload);
    });

    it('round-trips every short combination of dangerous characters', () => {
      const candidates = stringsUpToLength(["'", '\\', '\0', '\n', '\r', 'a'], 4);

      for (const candidate of candidates) {
        expect(decodeMySqlLiteral(mysql.escapeString(candidate))).toBe(candidate);
      }
    });

    it('escapes alternating backslashes and quote runs without changing their value', () => {
      const value = "\\''\\\\'''";
      expect(decodeMySqlLiteral(mysql.escapeString(value))).toBe(value);
    });

    it('preserves the full set of MySQL control-character inputs', () => {
      const value = "\0\b\n\r\t\\\x1a'\"";
      expect(decodeMySqlLiteral(mysql.escapeString(value))).toBe(value);
    });

    it('preserves representative Unicode and invisible characters', () => {
      const values = [
        'Café 日本語 👨‍👩‍👧‍👦',
        'cafe\u0301',
        'שלום\u00A0עולם',
        'left\u200Dright',
      ];

      for (const value of values) {
        expect(decodeMySqlLiteral(mysql.escapeString(value))).toBe(value);
      }
    });
  });

  describe('escapeString — Spark SQL/Hive/Databricks (backslash escapes only)', () => {
    it('escapes quotes with a backslash instead of doubling them', () => {
      // Verified against apache/spark:3.5.1 — `SELECT 'O''Brien'` returns OBrien
      // (two adjacent literals, silently concatenated), while `'O\'Brien'`
      // returns O'Brien. Hive 2.3.5 concatenates the same way by grammar.
      expect(spark.escapeString("O'Brien")).toBe("'O\\'Brien'");
      expect(spark.escapeString("O'Brien")).not.toContain("''");
    });

    it('doubles backslashes', () => {
      expect(spark.escapeString('a\\b')).toBe("'a\\\\b'");
    });

    it('neutralizes a trailing backslash + quote payload', () => {
      expect(spark.escapeString("\\'")).toBe("'\\\\\\''");
    });

    it.each(injectionPayloads)('keeps the %s payload inside one literal', (_name, payload) => {
      expect(decodeSparkLiteral(spark.escapeString(payload))).toBe(payload);
    });

    it('round-trips every short combination of dangerous characters', () => {
      const candidates = stringsUpToLength(["'", '\\', '\0', '\n', '\r', 'a'], 4);

      for (const candidate of candidates) {
        expect(decodeSparkLiteral(spark.escapeString(candidate))).toBe(candidate);
      }
    });

    it('escapes alternating backslashes and quote runs without changing their value', () => {
      const value = "\\''\\\\'''";
      expect(decodeSparkLiteral(spark.escapeString(value))).toBe(value);
    });

    it('preserves representative Unicode and invisible characters', () => {
      const values = [
        'Café 日本語 👨‍👩‍👧‍👦',
        'cafe\u0301',
        'مرحبا بالعالم',
        'left\u200Dright',
      ];

      for (const value of values) {
        expect(decodeSparkLiteral(spark.escapeString(value))).toBe(value);
      }
    });
  });

  describe('escapeIdentifier', () => {
    it('quotes with the dialect identifier char and doubles it', () => {
      expect(presto.escapeIdentifier('my"col')).toBe('"my""col"');
      expect(mysql.escapeIdentifier('my`col')).toBe('`my``col`');
      expect(spark.escapeIdentifier('my`col')).toBe('`my``col`');
    });

    it('keeps identifier injection payloads within the quoted identifier', () => {
      expect(presto.escapeIdentifier('users"; DROP TABLE users; --'))
        .toBe('"users""; DROP TABLE users; --"');
      expect(mysql.escapeIdentifier('users`; DROP TABLE users; #'))
        .toBe('`users``; DROP TABLE users; #`');
    });

    it('quotes dots as identifier content rather than allowing qualification', () => {
      expect(presto.escapeIdentifier('public.users')).toBe('"public.users"');
      expect(mysql.escapeIdentifier('public.users')).toBe('`public.users`');
    });

    it('doubles every delimiter in consecutive delimiter runs', () => {
      expect(presto.escapeIdentifier('a""b')).toBe('"a""""b"');
      expect(mysql.escapeIdentifier('a``b')).toBe('`a````b`');
    });

    it('quotes empty, reserved-word, and Unicode identifiers', () => {
      expect(presto.escapeIdentifier('')).toBe('""');
      expect(presto.escapeIdentifier('select')).toBe('"select"');
      expect(presto.escapeIdentifier('用户表')).toBe('"用户表"');
      expect(mysql.escapeIdentifier('')).toBe('``');
      expect(mysql.escapeIdentifier('select')).toBe('`select`');
      expect(mysql.escapeIdentifier('用户表')).toBe('`用户表`');
    });
  });

  describe('escapeValue', () => {
    it('handles primitives', () => {
      expect(presto.escapeValue(null)).toBe('NULL');
      expect(presto.escapeValue(undefined)).toBe('NULL');
      expect(presto.escapeValue(true)).toBe('TRUE');
      expect(presto.escapeValue(false)).toBe('FALSE');
      expect(presto.escapeValue(42)).toBe('42');
      expect(presto.escapeValue(-3.14)).toBe('-3.14');
      expect(presto.escapeValue(10n)).toBe('10');
      expect(presto.escapeValue("x'y")).toBe("'x''y'");
    });

    it('preserves sqlstring rendering for non-finite numbers', () => {
      expect(presto.escapeValue(Infinity)).toBe('Infinity');
      expect(presto.escapeValue(-Infinity)).toBe('-Infinity');
      expect(presto.escapeValue(NaN)).toBe('NaN');
    });

    it('renders arrays as a comma list', () => {
      expect(presto.escapeValue([1, "a'b", true])).toBe("1, 'a''b', TRUE");
      expect(presto.escapeValue([])).toBe('');
    });

    it('wraps nested arrays as sqlstring-compatible row groups', () => {
      expect(presto.escapeValue([[1, 2, 3]])).toBe('(1, 2, 3)');
      expect(presto.escapeValue([[[1]], [[2]]])).toBe('((1)), ((2))');
      expect(presto.format('WHERE id IN (?)', [[1, 2, 3]]))
        .toBe('WHERE id IN (1, 2, 3)');
      expect(presto.format('WHERE id IN (?)', [[[1, 2, 3]]]))
        .toBe('WHERE id IN ((1, 2, 3))');
      expect(presto.format('INSERT INTO t VALUES ?', [[[1, 'a'], [2, 'b']]]))
        .toBe("INSERT INTO t VALUES (1, 'a'), (2, 'b')");
    });

    it('recursively escapes injection payloads in nested arrays', () => {
      expect(presto.escapeValue(["x') OR TRUE --", ["'; DROP TABLE users; --"]]))
        .toBe("'x'') OR TRUE --', ('''; DROP TABLE users; --')");
    });

    it('renders valid dates as escaped strings and invalid dates as NULL', () => {
      expect(presto.escapeValue(new Date('2024-01-02T03:04:05.000Z')))
        .toBe("'2024-01-02T03:04:05.000Z'");
      expect(presto.escapeValue(new Date('invalid'))).toBe('NULL');
    });

    it('honors toSqlString escape hatch', () => {
      expect(presto.escapeValue({ toSqlString: () => 'NOW()' })).toBe('NOW()');
    });

    it('handles empty strings, numeric edges, and signed bigints', () => {
      expect(presto.escapeValue('')).toBe("''");
      expect(presto.escapeValue(0)).toBe('0');
      expect(presto.escapeValue(Number.MAX_SAFE_INTEGER)).toBe('9007199254740991');
      expect(presto.escapeValue(1.5e-5)).toBe('0.000015');
      expect(presto.escapeValue(-10n)).toBe('-10');
      expect(presto.escapeValue(0n)).toBe('0');
    });

    it('renders buffers as hexadecimal varbinary literals', () => {
      expect(presto.escapeValue(Buffer.from([0x00, 0xab, 0xff]))).toBe("X'00abff'");
      expect(mysql.escapeValue(Buffer.alloc(0))).toBe("X''");
    });

    it('rejects a plain object instead of expanding it into assignments', () => {
      expect(() => presto.escapeValue({ foo: "a'b", count: 2 })).toThrow(/Unsupported parameter type.*plain object/);
      expect(() => mysql.escapeValue({ 'odd`key': 'value' })).toThrow(/Unsupported parameter type/);
      expect(() => presto.escapeValue({})).toThrow(/Unsupported parameter type/);
      expect(() => presto.escapeValue({ toSqlString: 'OR TRUE' })).toThrow(/Unsupported parameter type/);
    });

    it('rejects a plain object nested in an array', () => {
      expect(() => presto.escapeValue([{ foo: 1 }])).toThrow(/Unsupported parameter type.*plain object/);
      expect(() => presto.escapeValue([[{ foo: 1 }]])).toThrow(/Unsupported parameter type.*plain object/);
      expect(presto.escapeValue([{ toSqlString: () => 'NOW()' }, 1])).toBe('NOW(), 1');
    });

    it('rejects values that cannot be represented as SQL literals', () => {
      expect(() => presto.escapeValue(Symbol('secret'))).toThrow(/Unsupported parameter type/);
      expect(() => presto.escapeValue(() => "'; DROP TABLE users; --"))
        .toThrow(/Unsupported parameter type/);
    });
  });

  describe('format', () => {
    it('substitutes ? with escaped values', () => {
      expect(presto.format('SELECT * FROM t WHERE a = ? AND b = ?', ["x'y", 5]))
        .toBe("SELECT * FROM t WHERE a = 'x''y' AND b = 5");
    });

    it('substitutes ?? with escaped identifiers', () => {
      expect(presto.format('SELECT ?? FROM t', ['col"1']))
        .toBe('SELECT "col""1" FROM t');
    });

    it('returns sql unchanged when no values', () => {
      expect(presto.format('SELECT 1', [])).toBe('SELECT 1');
      expect(presto.format('SELECT 1')).toBe('SELECT 1');
      expect(presto.format('SELECT ?', null)).toBe('SELECT ?');
    });

    it('normalizes a scalar values argument to one parameter like sqlstring', () => {
      expect(presto.format('SELECT ?', '')).toBe("SELECT ''");
      expect(presto.format('SELECT ?', 0)).toBe('SELECT 0');
      expect(presto.format('SELECT ?', false)).toBe('SELECT FALSE');
      expect(presto.format('SELECT ??', 'column"name')).toBe('SELECT "column""name"');
      expect(() => presto.format('SELECT ?, ?', 'whole string')).toThrow(/parameter count mismatch/);
    });

    it('closes the injection that the old PrestoDriver escaper allowed', () => {
      const out = presto.format('SELECT * FROM users WHERE name = ?', ["' OR 1=1 --"]);
      expect(out).toBe("SELECT * FROM users WHERE name = ''' OR 1=1 --'");
      // No unescaped quote precedes the injected OR.
      expect(out).not.toContain("\\'");
    });

    it('escapes malicious values and identifiers through their respective placeholders', () => {
      expect(presto.format('SELECT ?? FROM users WHERE name = ?', [
        'password" FROM admins; --',
        "' OR TRUE --",
      ])).toBe('SELECT "password"" FROM admins; --" FROM users WHERE name = \'\'\' OR TRUE --\'');
    });

    it('does not interpret placeholder-like text introduced by a value', () => {
      expect(presto.format('SELECT ? AS value, ? AS marker', [
        "?'; DROP TABLE users; --",
        'safe',
      ])).toBe("SELECT '?''; DROP TABLE users; --' AS value, 'safe' AS marker");
    });

    it('leaves unsupported placeholder runs intact without consuming a value', () => {
      expect(presto.format('SELECT ???, ?, ??', ['value', 'column"name']))
        .toBe('SELECT ???, \'value\', "column""name"');
    });

    it('rejects unmatched placeholders and surplus values (fail closed)', () => {
      expect(() => presto.format('SELECT ?, ?', ['first'])).toThrow(/parameter count mismatch/);
      expect(() => presto.format('SELECT ?', ['first', "'; DROP TABLE users; --"]))
        .toThrow(/parameter count mismatch/);
    });

    it('rejects values supplied for SQL with no usable placeholder (fail closed)', () => {
      expect(() => presto.format('SELECT 1', ["'; DROP TABLE users; --"]))
        .toThrow(/parameter count mismatch/);
      // `???` is not a consumable placeholder, so the value has nowhere to land.
      expect(() => presto.format('SELECT ???', ["'; DROP TABLE users; --"]))
        .toThrow(/parameter count mismatch/);
    });

    it('rejects an object bound to a filter placeholder', () => {
      // `tenant_id = "tenant_id" = TRUE` would match every row.
      expect(() => presto.format('SELECT * FROM t WHERE tenant_id = ?', [{ tenant_id: true }]))
        .toThrow(/Unsupported parameter type.*plain object/);
    });

    it('substitutes placeholders adjacent to arithmetic operators', () => {
      expect(presto.format('SELECT ? - ? + ? / ?', [10, 3, 20, 4]))
        .toBe('SELECT 10 - 3 + 20 / 4');
    });
  });

  describe('format (dialect-driven entry point)', () => {
    it('escapes according to the requested dialect', () => {
      const value = "a\\b'c";

      expect(format('ansi', 'WHERE a = ?', [value])).toBe("WHERE a = 'a\\b''c'");
      expect(format('mysql', 'WHERE a = ?', [value])).toBe("WHERE a = 'a\\\\b''c'");
      expect(format('spark', 'WHERE a = ?', [value])).toBe("WHERE a = 'a\\\\b\\'c'");
    });

    it('escapes identifiers according to the requested dialect', () => {
      expect(format('ansi', 'SELECT ??', ['col'])).toBe('SELECT "col"');
      expect(format('mysql', 'SELECT ??', ['col'])).toBe('SELECT `col`');
      expect(format('spark', 'SELECT ??', ['col'])).toBe('SELECT `col`');
    });

    it('backs the per-dialect convenience helpers', () => {
      const args: [string, unknown[]] = ['WHERE a = ? AND b = ??', ["it's", 'col']];

      expect(formatAnsi(...args)).toBe(format('ansi', ...args));
      expect(formatMySql(...args)).toBe(format('mysql', ...args));
      expect(formatSparkSql(...args)).toBe(format('spark', ...args));
    });
  });

  describe('formatAnsi / formatMySql convenience helpers', () => {
    it('formatAnsi escapes as standard SQL (quotes doubled, backslash literal)', () => {
      expect(formatAnsi('WHERE a = ? AND b = ?', ["a\\b'c", 5]))
        .toBe("WHERE a = 'a\\b''c' AND b = 5");
    });

    it('formatMySql escapes backslashes as well', () => {
      expect(formatMySql('WHERE a = ?', ["a\\b'c"]))
        .toBe("WHERE a = 'a\\\\b''c'");
    });

    it('formatAnsi neutralizes the quote-breakout payload', () => {
      expect(formatAnsi('WHERE name = ?', ["' OR 1=1 --"]))
        .toBe("WHERE name = ''' OR 1=1 --'");
    });

    it('helpers tolerate missing values', () => {
      expect(formatAnsi('SELECT 1')).toBe('SELECT 1');
      expect(formatMySql('SELECT 1')).toBe('SELECT 1');
    });

    it('helpers normalize scalar values', () => {
      expect(formatMySql('SELECT ?', false)).toBe('SELECT FALSE');
      expect(() => formatAnsi('SELECT ?, ?', "a'b")).toThrow(/parameter count mismatch/);
    });
  });

  describe('fail-closed parameter count guard', () => {
    it('does not let a quoted ? shift real placeholders (FILTER_PARAMS vector)', () => {
      // Model SQL with a literal `?` ahead of real placeholders: a naive scan would
      // land the attacker's value inside the literal and comment out the tail.
      expect(formatAnsi("WHERE (status = '?') AND status = ? AND number = ?", [' OR 1=1) --', '7']))
        .toBe("WHERE (status = '?') AND status = ' OR 1=1) --' AND number = '7'");
    });

    it('throws when a literal ? in code position shifts real placeholders', () => {
      expect(() => formatAnsi('WHERE a ? b AND status = ? AND number = ?', ['x', '7']))
        .toThrow(/parameter count mismatch/);
    });

    it('throws on surplus values', () => {
      expect(() => formatAnsi('WHERE a = ?', ['x', 'y'])).toThrow(/parameter count mismatch/);
    });

    it('reports both counts in the error', () => {
      expect(() => formatAnsi('WHERE a = ? AND b = ?', ['x']))
        .toThrow(/2 placeholder\(s\) but 1 value\(s\)/);
      expect(() => formatAnsi('WHERE a = ?', ['x', 'y', 'z']))
        .toThrow(/1 placeholder\(s\) but 3 value\(s\)/);
      // A quoted `?` and a `???` run are not counted.
      expect(() => formatClickHouse("WHERE a = '?' AND b = ??? AND c = ?", ['x', 'y']))
        .toThrow(/1 placeholder\(s\) but 2 value\(s\)/);
    });

    it('throws on too few values', () => {
      expect(() => formatAnsi('WHERE a = ? AND b = ?', ['x'])).toThrow(/parameter count mismatch/);
    });

    it('accepts a balanced placeholder/value count', () => {
      expect(formatAnsi('WHERE a = ? AND b = ?', ['x', 'y']))
        .toBe("WHERE a = 'x' AND b = 'y'");
    });

    it('counts ?? identifier placeholders as consumable', () => {
      expect(formatAnsi('SELECT ??, ?', ['col', 'v'])).toBe('SELECT "col", \'v\'');
      expect(() => formatAnsi('SELECT ??, ?', ['col'])).toThrow(/parameter count mismatch/);
    });

    it('leaves the no-substitution paths non-throwing', () => {
      expect(formatAnsi('SELECT 1')).toBe('SELECT 1');
      expect(formatAnsi('SELECT 1', [])).toBe('SELECT 1');
      expect(formatAnsi('SELECT ???', [])).toBe('SELECT ???');
      expect(formatAnsi('SELECT ?')).toBe('SELECT ?');
      expect(formatAnsi('SELECT ?', null)).toBe('SELECT ?');
      expect(formatAnsi('SELECT ?', undefined)).toBe('SELECT ?');
    });

    it('returns the SQL unchanged for an empty values array', () => {
      expect(formatAnsi('SELECT ?', [])).toBe('SELECT ?');
      expect(formatAnsi('SELECT ??, ?', [])).toBe('SELECT ??, ?');
    });

    it('keeps a ? operator in a query without parameters', () => {
      const ternary = 'SELECT if_null > 0 ? 1 : 0 FROM t';

      expect(formatClickHouse(ternary, [])).toBe(ternary);
      expect(formatClickHouse(ternary)).toBe(ternary);
    });

    it('rejects a ? operator next to real placeholders', () => {
      expect(() => formatClickHouse('SELECT a > 0 ? 1 : 0 FROM t WHERE b = ?', ['v']))
        .toThrow(/parameter count mismatch/);
    });
  });

  describe('literal ? outside code position', () => {
    it('keeps ? inside string literals, with and without values', () => {
      expect(formatAnsi("SELECT regexp_like(a, 'x+?') FROM t", []))
        .toBe("SELECT regexp_like(a, 'x+?') FROM t");
      expect(formatAnsi("SELECT ?, regexp_like(a, 'x+?')", ['v']))
        .toBe("SELECT 'v', regexp_like(a, 'x+?')");
      expect(formatAnsi("WHERE split_part(url, '?', 1) = ? AND b LIKE 'y??'", ['v']))
        .toBe("WHERE split_part(url, '?', 1) = 'v' AND b LIKE 'y??'");
    });

    it('treats a doubled quote as part of the literal', () => {
      expect(formatAnsi("WHERE a = 'it''s?' AND b = ?", ['v']))
        .toBe("WHERE a = 'it''s?' AND b = 'v'");
      expect(formatAnsi("WHERE a = '' AND b = ?", ['v'])).toBe("WHERE a = '' AND b = 'v'");
    });

    it('keeps ? inside quoted identifiers', () => {
      expect(formatAnsi('SELECT "is it?" FROM t WHERE a = ?', ['v']))
        .toBe('SELECT "is it?" FROM t WHERE a = \'v\'');
      expect(formatMySql('SELECT `is it?`, `a\\` FROM t WHERE a = ?', ['v']))
        .toBe('SELECT `is it?`, `a\\` FROM t WHERE a = \'v\'');
    });

    it('keeps ? inside comments, where an apostrophe opens no literal', () => {
      expect(formatAnsi("SELECT 1 -- isn't it?\nWHERE a = ?", ['v']))
        .toBe("SELECT 1 -- isn't it?\nWHERE a = 'v'");
      expect(formatAnsi("SELECT /* why? don't */ ? FROM t", ['v']))
        .toBe("SELECT /* why? don't */ 'v' FROM t");
      expect(formatAnsi("SELECT '--?', '/*', ? FROM t", ['v']))
        .toBe("SELECT '--?', '/*', 'v' FROM t");
    });

    it('honours backslash escapes in ClickHouse backtick identifiers', () => {
      // The form ClickHouseQuery emits for a column named a`b.
      expect(formatClickHouse('SELECT count(*) AS `a\\`b` FROM t WHERE status = ? AND n = ?', ['x', 7]))
        .toBe("SELECT count(*) AS `a\\`b` FROM t WHERE status = 'x' AND n = 7");
      expect(formatClickHouse('SELECT `a\\\\`, `is it?`, "b\\"?" FROM t WHERE a = ?', ['v']))
        .toBe('SELECT `a\\\\`, `is it?`, "b\\"?" FROM t WHERE a = \'v\'');
    });

    it('formats ClickHouse values like MySQL and keeps # comments', () => {
      expect(formatClickHouse('WHERE a = ?', ["a\\b'c"])).toBe(formatMySql('WHERE a = ?', ["a\\b'c"]));
      expect(formatClickHouse("SELECT 1 # why? it's\nWHERE a = ?", ['v']))
        .toBe("SELECT 1 # why? it's\nWHERE a = 'v'");
    });

    it('doubles backslashes in ClickHouse identifier placeholders', () => {
      // A trailing backslash would otherwise escape the closing backtick.
      expect(formatClickHouse('SELECT ??', ['a\\'])).toBe('SELECT `a\\\\`');
      expect(formatClickHouse('SELECT ??', ['a`b'])).toBe('SELECT `a``b`');
      expect(formatMySql('SELECT ??', ['a\\'])).toBe('SELECT `a\\`');
    });

    it('requires whitespace after -- for a MySQL comment', () => {
      // MySQL reads `a--1` as `a - -1`, so the `?` is a real placeholder.
      expect(formatMySql('SELECT a--? FROM t', [1])).toBe('SELECT a--1 FROM t');
      expect(formatMySql("SELECT 1 -- why? it's\nWHERE a = ?", ['v']))
        .toBe("SELECT 1 -- why? it's\nWHERE a = 'v'");
      expect(formatMySql('SELECT 1 --\twhy?\nWHERE a = ?', ['v'])).toBe("SELECT 1 --\twhy?\nWHERE a = 'v'");
      expect(formatMySql('SELECT 1 --\nWHERE a = ?', ['v'])).toBe("SELECT 1 --\nWHERE a = 'v'");
      expect(formatMySql('SELECT ? --', ['v'])).toBe("SELECT 'v' --");
      expect(() => formatMySql('SELECT 1 -- AND a = ?', ['v'])).toThrow(/parameter count mismatch/);
    });

    it('starts a comment at a bare -- for the other dialects', () => {
      expect(formatClickHouse('SELECT 1--why?\n, ?', ['v'])).toBe("SELECT 1--why?\n, 'v'");
      expect(formatAnsi('SELECT 1--why?\n, ?', ['v'])).toBe("SELECT 1--why?\n, 'v'");
      expect(formatSparkSql('SELECT 1--why?\n, ?', ['v'])).toBe("SELECT 1--why?\n, 'v'");
    });

    it('tracks nested block comments for ClickHouse', () => {
      // Closing at the first terminator would count the `?` in the outer comment, let
      // `don't` hide the real placeholder, and put the value where its own terminator
      // ends the comment.
      const sql = "SELECT 1 /* outer /* inner */ ? don't */, 2 WHERE 1 = ? AND 'x' = 'x'";

      expect(formatClickHouse(sql, ['*/, 42 --']))
        .toBe("SELECT 1 /* outer /* inner */ ? don't */, 2 WHERE 1 = '*/, 42 --' AND 'x' = 'x'");
      expect(formatClickHouse('SELECT /* a /* b /* c */ ? */ ? */ ?', ['v'])).toBe("SELECT /* a /* b /* c */ ? */ ? */ 'v'");
      expect(formatClickHouse('SELECT /*/ ? */ ?', ['v'])).toBe("SELECT /*/ ? */ 'v'");
      expect(() => formatClickHouse('SELECT /* a /* b */ ?', ['v'])).toThrow(/0 placeholder\(s\) but 1 value\(s\)/);
    });

    it('tracks nested block comments for Spark', () => {
      expect(formatSparkSql("SELECT 1 /* outer /* inner */ ? don't */, 2 WHERE 1 = ? AND 'x' = 'x'", ['*/, 42 --']))
        .toBe("SELECT 1 /* outer /* inner */ ? don't */, 2 WHERE 1 = '*/, 42 --' AND 'x' = 'x'");
      expect(() => formatSparkSql('SELECT /* a /* b */ ?', ['v'])).toThrow(/0 placeholder\(s\) but 1 value\(s\)/);
    });

    it('closes a block comment at the first terminator for Trino and MySQL', () => {
      expect(formatAnsi('SELECT /* a /* b */ ?', ['v'])).toBe("SELECT /* a /* b */ 'v'");
      expect(formatMySql('SELECT /* a /* b */ ?', ['v'])).toBe("SELECT /* a /* b */ 'v'");
    });

    it('skips ClickHouse heredoc strings', () => {
      // Scanned as code, the `?` would be counted and `it's` would hide the real placeholder.
      expect(formatClickHouse("SELECT $$ what? it's $$ WHERE a = ? AND b = 'x'", ['v $$, 42 --']))
        .toBe("SELECT $$ what? it's $$ WHERE a = 'v $$, 42 --' AND b = 'x'");
      expect(formatClickHouse("SELECT $tag$ ? $$ ' $tag$, ?", ['v'])).toBe("SELECT $tag$ ? $$ ' $tag$, 'v'");
      expect(formatClickHouse('SELECT 1 AS a$b$c, ? AS d$b$', ['v'])).toBe("SELECT 1 AS a$b$c, 'v' AS d$b$");
      expect(() => formatClickHouse('SELECT $$ open ?', ['v'])).toThrow(/0 placeholder\(s\) but 1 value\(s\)/);
      expect(formatAnsi('SELECT $$ ? $$', ['v'])).toBe("SELECT $$ 'v' $$");
    });

    it('ends a line comment at a lone carriage return for Trino and Spark', () => {
      expect(formatAnsi('SELECT 1 -- why\r, ?', ['v'])).toBe("SELECT 1 -- why\r, 'v'");
      expect(formatSparkSql('SELECT 1 -- why\r, ?', ['v'])).toBe("SELECT 1 -- why\r, 'v'");
      expect(formatAnsi('SELECT 1 -- why?\r\n, ?', ['v'])).toBe("SELECT 1 -- why?\r\n, 'v'");
      // MySQL and ClickHouse keep the comment open until the line feed.
      expect(() => formatClickHouse('SELECT 1 -- why\r, ?', ['v'])).toThrow(/parameter count mismatch/);
      expect(() => formatMySql('SELECT 1 -- why\r, ?', ['v'])).toThrow(/parameter count mismatch/);
    });

    it('carries a Spark line comment over a backslash-newline', () => {
      expect(formatSparkSql('SELECT 1 -- why \\\n still? \\\n more?\n, ?', ['v']))
        .toBe("SELECT 1 -- why \\\n still? \\\n more?\n, 'v'");
      expect(formatSparkSql('SELECT 1 --\n, ?', ['v'])).toBe("SELECT 1 --\n, 'v'");
      expect(() => formatSparkSql('SELECT 1 -- why \\\n, ?', ['v'])).toThrow(/parameter count mismatch/);
      expect(formatAnsi('SELECT 1 -- why \\\n, ?', ['v'])).toBe("SELECT 1 -- why \\\n, 'v'");
      expect(formatClickHouse('SELECT 1 -- why \\\n, ?', ['v'])).toBe("SELECT 1 -- why \\\n, 'v'");
    });

    it('treats # as a line comment only for dialects that have it', () => {
      expect(formatMySql("SELECT 1 # why? it's\nWHERE a = ? AND b = 'x'", ['v']))
        .toBe("SELECT 1 # why? it's\nWHERE a = 'v' AND b = 'x'");
      expect(formatMySql("SELECT '#?', ? FROM t", ['v'])).toBe("SELECT '#?', 'v' FROM t");
      // Trino and Spark have no `#` comment, so the `?` after it is in code position.
      expect(formatAnsi('SELECT a # ? FROM t', ['v'])).toBe("SELECT a # 'v' FROM t");
      expect(formatSparkSql('SELECT a # ? FROM t', ['v'])).toBe("SELECT a # 'v' FROM t");
    });

    it('rejects a placeholder inside a comment instead of substituting into it', () => {
      // A value with a newline would end the line comment and become live SQL.
      expect(() => formatAnsi('SELECT 1 -- AND a = ?', ['x\n OR 1=1']))
        .toThrow(/parameter count mismatch/);
      expect(() => formatMySql('SELECT 1 # AND a = ?', ['x\n OR 1=1']))
        .toThrow(/parameter count mismatch/);
    });

    it('follows the dialect on backslash escapes', () => {
      // ANSI: the backslash is data, so the literal ends at the next quote.
      expect(formatAnsi("WHERE a = 'x\\' AND b = ?", ['v'])).toBe("WHERE a = 'x\\' AND b = 'v'");
      // MySQL / Spark: `\'` stays inside the literal, and so does the `?` after it.
      expect(formatMySql("WHERE a = 'x\\'?' AND b = ?", ['v'])).toBe("WHERE a = 'x\\'?' AND b = 'v'");
      expect(formatSparkSql("WHERE a = 'x\\\\' AND b = ?", ['v']))
        .toBe("WHERE a = 'x\\\\' AND b = 'v'");
    });

    it('still rejects ? in code position and in unterminated regions', () => {
      expect(() => formatAnsi('SELECT a ? b WHERE c = ?', ['v'])).toThrow(/parameter count mismatch/);
      expect(() => formatAnsi("WHERE a = 'open AND b = ?", ['v'])).toThrow(/parameter count mismatch/);
      expect(() => formatAnsi('SELECT /* open ?', ['v'])).toThrow(/parameter count mismatch/);
    });
  });

  describe('escapeStringLiteral', () => {
    it('wraps the value in quotes', () => {
      expect(escapeStringLiteral('orders_rollup')).toBe("'orders_rollup'");
    });

    it('doubles an embedded quote so the literal cannot be escaped', () => {
      expect(escapeStringLiteral("pre_agg_x'; DROP TABLE victim; --"))
        .toBe("'pre_agg_x''; DROP TABLE victim; --'");
    });

    it('leaves a backslash as data', () => {
      expect(escapeStringLiteral('folder\\name')).toBe("'folder\\name'");
      expect(escapeStringLiteral('trailing\\')).toBe("'trailing\\'");
    });

    it.each(injectionPayloads)('neutralizes the %s payload', (_name, payload) => {
      expect(decodeAnsiLiteral(escapeStringLiteral(payload))).toBe(payload);
    });

    it('round-trips every short combination of dangerous characters', () => {
      const candidates = stringsUpToLength(["'", '\\', '\0', '\n', '\r', 'a'], 4);

      for (const candidate of candidates) {
        expect(decodeAnsiLiteral(escapeStringLiteral(candidate))).toBe(candidate);
      }
    });
  });

  describe('escapeIdentifierAnsi / escapeIdentifierMySql / escapeIdentifierSparkSql', () => {
    it('doubles the dialect identifier quote', () => {
      expect(escapeIdentifierAnsi('a"b')).toBe('"a""b"');
      expect(escapeIdentifierMySql('x` FROM (SELECT 1) t -- ')).toBe('`x`` FROM (SELECT 1) t -- `');
      expect(escapeIdentifierSparkSql('a`b')).toBe('`a``b`');
    });

    it('escapes backslashes before backticks for backslash-escaping engines', () => {
      expect(escapeIdentifierBackslash('a`b')).toBe('`a\\`b`');
      expect(escapeIdentifierBackslash('a\\`b')).toBe('`a\\\\\\`b`');
      expect(escapeIdentifierBackslash('a\\')).toBe('`a\\\\`');
    });
  });
});
