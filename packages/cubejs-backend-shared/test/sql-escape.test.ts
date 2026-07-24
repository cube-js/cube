/* eslint-disable quotes */
import {
  SqlEscaper,
  formatAnsi,
  formatMySql,
  AnsiSqlDialect,
  MySqlDialect,
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
  const presto = new SqlEscaper(AnsiSqlDialect); // ANSI rules (Presto/Trino/Postgres)
  const mysql = new SqlEscaper(MySqlDialect);

  describe('SqlEscaper construction', () => {
    it('exposes the built-in dialect presets', () => {
      expect(AnsiSqlDialect.escapeBackslash).toBe(false);
      expect(MySqlDialect.escapeBackslash).toBe(true);
    });

    it('accepts explicit rules', () => {
      const e = SqlEscaper.forDialect({
        stringQuoteChar: "'",
        doubleQuoteToEscape: true,
        escapeBackslash: false,
        identifierQuoteChar: '"',
      });
      expect(e.escapeString("a'b")).toBe("'a''b'");
    });

    it('supports dialects that escape quotes with backslashes', () => {
      const e = SqlEscaper.forDialect({
        stringQuoteChar: "'",
        doubleQuoteToEscape: false,
        escapeBackslash: true,
        identifierQuoteChar: '`',
      });
      expect(e.escapeString("a\\'b")).toBe("'a\\\\\\'b'");
    });
  });

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

  describe('escapeIdentifier', () => {
    it('quotes with the dialect identifier char and doubles it', () => {
      expect(presto.escapeIdentifier('my"col')).toBe('"my""col"');
      expect(mysql.escapeIdentifier('my`col')).toBe('`my``col`');
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

    it('rejects non-finite numbers', () => {
      expect(() => presto.escapeValue(Infinity)).toThrow(/non-finite/);
      expect(() => presto.escapeValue(-Infinity)).toThrow(/non-finite/);
      expect(() => presto.escapeValue(NaN)).toThrow(/non-finite/);
    });

    it('renders arrays as a comma list', () => {
      expect(presto.escapeValue([1, "a'b", true])).toBe("1, 'a''b', TRUE");
      expect(presto.escapeValue([])).toBe('');
    });

    it('recursively escapes injection payloads in nested arrays', () => {
      expect(presto.escapeValue(["x') OR TRUE --", ["'; DROP TABLE users; --"]]))
        .toBe("'x'') OR TRUE --', '''; DROP TABLE users; --'");
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

    it('rejects unsupported objects', () => {
      expect(() => presto.escapeValue({ foo: 1 })).toThrow(/Unsupported object/);
      expect(() => presto.escapeValue({ toSqlString: 'DROP TABLE users' }))
        .toThrow(/Unsupported object/);
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

    it('leaves unmatched placeholders and ignores surplus values', () => {
      expect(presto.format('SELECT ?, ?', ['first'])).toBe("SELECT 'first', ?");
      expect(presto.format('SELECT ?', ['first', "'; DROP TABLE users; --"]))
        .toBe("SELECT 'first'");
    });

    it('returns SQL unchanged when supplied values have no usable placeholder', () => {
      expect(presto.format('SELECT 1', ["'; DROP TABLE users; --"])).toBe('SELECT 1');
      expect(presto.format('SELECT ???', ["'; DROP TABLE users; --"])).toBe('SELECT ???');
    });

    it('rejects JSON-shaped objects instead of expanding them into query structure', () => {
      expect(() => presto.format('DELETE FROM entries WHERE id = ?', [{ id: true }]))
        .toThrow(/Unsupported object/);
      expect(() => presto.format('SELECT ?', [{ toSqlString: 'OR TRUE' }]))
        .toThrow(/Unsupported object/);
    });

    it('substitutes placeholders adjacent to arithmetic operators', () => {
      expect(presto.format('SELECT ? - ? + ? / ?', [10, 3, 20, 4]))
        .toBe('SELECT 10 - 3 + 20 / 4');
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
  });
});
