export interface SqlDialectEscapeRules {
  /** Character that delimits string literals. Standard SQL uses a single quote. */
  readonly stringQuoteChar: string;
  /**
   * When `true`, a quote inside a literal is escaped by doubling it (`''`).
   * Every standards-tracking engine supports this, so it is the safe default.
   */
  readonly doubleQuoteToEscape: boolean;
  /**
   * When `true`, the backslash is an escape character inside string literals and
   * a literal backslash must be doubled (`\\`). MySQL behaves this way; Presto,
   * Trino and standard SQL do NOT.
   */
  readonly escapeBackslash: boolean;
  /** Character that delimits identifiers (`"` for standard SQL, `` ` `` for MySQL). */
  readonly identifierQuoteChar: string;
  /** When `true`, `#` starts a line comment. MySQL and ClickHouse accept it. */
  readonly hashLineComments: boolean;
  /**
   * When `true`, the backslash is an escape character inside quoted identifiers
   * too. ClickHouse behaves this way; in MySQL it is ordinary text there.
   */
  readonly backslashInQuotedIdentifiers: boolean;
  /**
   * When `true`, `--` starts a line comment only before whitespace, a control
   * character or the end of the SQL. MySQL reads `1--1` as `1 - -1`.
   */
  readonly dashCommentNeedsSpace: boolean;
}

const AnsiSqlDialect: SqlDialectEscapeRules = {
  stringQuoteChar: '\'',
  doubleQuoteToEscape: true,
  escapeBackslash: false,
  identifierQuoteChar: '"',
  hashLineComments: false,
  backslashInQuotedIdentifiers: false,
  dashCommentNeedsSpace: false,
};

const MySqlDialect: SqlDialectEscapeRules = {
  stringQuoteChar: '\'',
  doubleQuoteToEscape: true,
  escapeBackslash: true,
  identifierQuoteChar: '`',
  hashLineComments: true,
  backslashInQuotedIdentifiers: false,
  dashCommentNeedsSpace: true,
};

/**
 * ClickHouse. The backslash is an escape character in string literals and in
 * backtick identifiers alike: `` `a\`b` `` is one identifier, which is the form
 * `ClickHouseQuery` emits. A comment starts at `#` or at `--` with nothing after it.
 */
const ClickHouseDialect: SqlDialectEscapeRules = {
  stringQuoteChar: '\'',
  doubleQuoteToEscape: true,
  escapeBackslash: true,
  identifierQuoteChar: '`',
  hashLineComments: true,
  backslashInQuotedIdentifiers: true,
  dashCommentNeedsSpace: false,
};

/**
 * Spark SQL / Hive / Databricks. Their lexers know backslash escapes only — a
 * doubled quote is NOT an escape, it closes the literal and opens the next one.
 * Because adjacent literals are concatenated, `'O''Brien'` silently evaluates to
 * `OBrien` instead of raising, so quotes MUST be escaped as `\'` here.
 */
const SparkSqlDialect: SqlDialectEscapeRules = {
  stringQuoteChar: '\'',
  doubleQuoteToEscape: false,
  escapeBackslash: true,
  identifierQuoteChar: '`',
  hashLineComments: false,
  backslashInQuotedIdentifiers: false,
  dashCommentNeedsSpace: false,
};

class SqlEscaper {
  public constructor(public readonly rules: SqlDialectEscapeRules) {}

  public static forDialect(dialect: SqlDialectEscapeRules): SqlEscaper {
    return new SqlEscaper(dialect);
  }

  public escapeString(value: string): string {
    const { stringQuoteChar, escapeBackslash, doubleQuoteToEscape } = this.rules;

    let escaped = value;
    // Backslash MUST be doubled first (before quote handling) so we do not double
    // the backslashes we ourselves introduce when escaping quotes.
    if (escapeBackslash) {
      escaped = escaped.split('\\').join('\\\\');
    }

    escaped = doubleQuoteToEscape
      ? escaped.split(stringQuoteChar).join(stringQuoteChar + stringQuoteChar)
      : escaped.split(stringQuoteChar).join(`\\${stringQuoteChar}`);

    return `${stringQuoteChar}${escaped}${stringQuoteChar}`;
  }

  public escapeIdentifier(identifier: string): string {
    const q = this.rules.identifierQuoteChar;

    let escaped = String(identifier);
    if (this.rules.backslashInQuotedIdentifiers) {
      escaped = escaped.split('\\').join('\\\\');
    }

    return `${q}${escaped.split(q).join(q + q)}${q}`;
  }

  public escapeValue(value: unknown): string {
    return this.escapeValueInternal(value, false);
  }

  private escapeValueInternal(value: unknown, stringifyObjects: boolean): string {
    if (value === null || value === undefined) {
      return 'NULL';
    }

    switch (typeof value) {
      case 'boolean':
        return value ? 'TRUE' : 'FALSE';
      case 'bigint':
        return value.toString();
      case 'number':
        return String(value);
      case 'string':
        return this.escapeString(value);
      case 'object': {
        const obj = value as { toSqlString?: () => unknown };
        if (value instanceof Date) {
          return Number.isNaN(value.getTime()) ? 'NULL' : this.escapeString(value.toISOString());
        }

        if (Array.isArray(value)) {
          return value
            .map((v) => (
              Array.isArray(v)
                ? `(${this.escapeValueInternal(v, true)})`
                : this.escapeValueInternal(v, true)
            ))
            .join(', ');
        }

        if (Buffer.isBuffer(value)) {
          return `X${this.escapeString(value.toString('hex'))}`;
        }

        if (typeof obj.toSqlString === 'function') {
          return String(obj.toSqlString());
        }

        if (stringifyObjects) {
          return this.escapeString(String(value));
        }

        return Object.keys(value)
          .filter((key) => typeof (value as Record<string, unknown>)[key] !== 'function')
          .map((key) => (
            `${this.escapeIdentifier(key)} = ${
              this.escapeValueInternal((value as Record<string, unknown>)[key], true)
            }`
          ))
          .join(', ');
      }
      default:
        throw new Error(`Unsupported parameter type for SQL escaping: ${typeof value}`);
    }
  }

  /**
   * Substitutes positional placeholders in `sql` with escaped `values`:
   * - `?`  is replaced by an escaped value ({@link escapeValue})
   * - `??` is replaced by an escaped identifier ({@link escapeIdentifier})
   *
   * A `?` inside a quoted region (string literal, quoted identifier) or a comment
   * is SQL text, not a placeholder: the compiler only ever emits placeholders in
   * code position, so `regexp_like(a, 'x+?')` needs no escaping.
   */
  public format(sql: string, values?: unknown): string {
    if (values === null || values === undefined) {
      return sql;
    }

    const valueList = Array.isArray(values) ? values : [values];

    // Nothing to substitute, so nothing can land in the wrong slot. Scanning anyway
    // would reject a `?` that is an operator, like the ClickHouse `cond ? a : b`.
    if (valueList.length === 0) {
      return sql;
    }

    const tokens = this.placeholderScanner();

    let result = '';
    let chunkIndex = 0;
    let valuesIndex = 0;
    let placeholderCount = 0;

    let match: RegExpExecArray | null;

    // eslint-disable-next-line no-cond-assign
    while ((match = tokens.exec(sql)) !== null) {
      const token = match[0];

      if (token[0] !== '?') {
        tokens.lastIndex = this.skipNonCode(sql, match.index, token);
      } else if (token.length <= 2) {
        // `?` -> value, `??` -> identifier. Longer runs (`???`+) are not placeholders
        // we understand, so we leave them verbatim and consume no value.
        placeholderCount += 1;

        if (valuesIndex < valueList.length) {
          const rendered = token.length === 2
            ? this.escapeIdentifier(String(valueList[valuesIndex]))
            : this.escapeValue(valueList[valuesIndex]);

          result += sql.slice(chunkIndex, match.index) + rendered;
          chunkIndex = tokens.lastIndex;
          valuesIndex += 1;
        }
      }
    }

    if (placeholderCount !== valueList.length) {
      throw SqlEscaper.parameterCountMismatch(placeholderCount, valueList.length);
    }

    if (chunkIndex === 0) {
      return sql;
    }

    return chunkIndex < sql.length ? result + sql.slice(chunkIndex) : result;
  }

  /**
   * Matches placeholder runs and the openers of quoted regions and comments, as
   * the dialect defines them.
   */
  protected placeholderScanner(): RegExp {
    const { hashLineComments, dashCommentNeedsSpace } = this.rules;

    const quoteOrHash = hashLineComments ? '[\'"`#]' : '[\'"`]';
    // Space is the last character MySQL accepts after `--`, so anything from `!` up rules a comment out.
    const dashComment = dashCommentNeedsSpace ? '--(?![!-\\uffff])' : '--';

    return new RegExp(`\\?+|${quoteOrHash}|${dashComment}|\\/\\*`, 'g');
  }

  /**
   * Returns the index just past the quoted region or comment opened at `start`.
   * Backslash handling must match the engine, as in {@link escapeString}: both follow `rules`.
   */
  protected skipNonCode(sql: string, start: number, opener: string): number {
    if (opener === '--' || opener === '#') {
      const lineEnd = sql.indexOf('\n', start + opener.length);
      return lineEnd === -1 ? sql.length : lineEnd + 1;
    }

    if (opener === '/*') {
      const commentEnd = sql.indexOf('*/', start + 2);
      return commentEnd === -1 ? sql.length : commentEnd + 2;
    }

    const { escapeBackslash, backslashInQuotedIdentifiers } = this.rules;

    if (!escapeBackslash || (opener === '`' && !backslashInQuotedIdentifiers)) {
      const close = sql.indexOf(opener, start + 1);
      return close === -1 ? sql.length : close + 1;
    }

    let i = start + 1;
    while (i < sql.length) {
      if (sql[i] === opener) {
        return i + 1;
      }

      i += sql[i] === '\\' ? 2 : 1;
    }

    return sql.length;
  }

  protected static parameterCountMismatch(placeholderCount: number, valueCount: number): Error {
    return new Error(
      `SQL parameter count mismatch: ${placeholderCount} placeholder(s) but ${valueCount} value(s) supplied. ` +
      'A literal \'?\' is only allowed inside a string literal, a quoted identifier or a comment.'
    );
  }
}

/**
 * Escaping dialect of the target engine:
 *   - `ansi`  — standard SQL: Presto, Trino, Athena, Dremio, ksqlDB, Pinot, ...
 *   - `mysql` — MySQL / MariaDB: quotes doubled, backslash is an escape char
 *   - `clickhouse` — ClickHouse: quotes doubled, backslash escapes in literals and identifiers
 *   - `spark` — Spark SQL / Hive / Databricks: backslash escapes only
 */
export type EscapeDialect = 'ansi' | 'mysql' | 'clickhouse' | 'spark';

const Dialects: Record<EscapeDialect, SqlDialectEscapeRules> = {
  ansi: AnsiSqlDialect,
  mysql: MySqlDialect,
  clickhouse: ClickHouseDialect,
  spark: SparkSqlDialect,
};

/**
 * Format a query for the given engine dialect: `?` -> escaped value,
 * `??` -> escaped identifier.
 */
export function format(dialect: EscapeDialect, sql: string, values?: unknown): string {
  return new SqlEscaper(Dialects[dialect]).format(sql, values);
}

/**
 * Format a query for standard-SQL engines (Presto, Trino, Postgres, ...):
 * `?` -> escaped value, `??` -> escaped identifier.
 */
export function formatAnsi(sql: string, values?: unknown): string {
  return format('ansi', sql, values);
}

/**
 * Format a query for MySQL / MariaDB: `?` -> escaped value, `??` -> escaped
 * identifier.
 */
export function formatMySql(sql: string, values?: unknown): string {
  return format('mysql', sql, values);
}

/**
 * Format a query for ClickHouse: `?` -> escaped value, `??` -> escaped identifier.
 */
export function formatClickHouse(sql: string, values?: unknown): string {
  return format('clickhouse', sql, values);
}

/**
 * Format a query for Spark SQL / Hive / Databricks: `?` -> escaped value,
 * `??` -> escaped identifier.
 */
export function formatSparkSql(sql: string, values?: unknown): string {
  return format('spark', sql, values);
}

/**
 * Quote an identifier for standard-SQL engines (Presto, Trino, Postgres, ...): `"a""b"`.
 */
export function escapeIdentifierAnsi(identifier: string): string {
  return new SqlEscaper(AnsiSqlDialect).escapeIdentifier(identifier);
}

/**
 * Quote an identifier for MySQL / MariaDB: `` `a``b` ``.
 */
export function escapeIdentifierMySql(identifier: string): string {
  return new SqlEscaper(MySqlDialect).escapeIdentifier(identifier);
}

/**
 * Quote an identifier for Spark SQL / Hive / Databricks: `` `a``b` ``.
 */
export function escapeIdentifierSparkSql(identifier: string): string {
  return new SqlEscaper(SparkSqlDialect).escapeIdentifier(identifier);
}

export function escapeIdentifierBackslash(identifier: string): string {
  return `\`${String(identifier).replace(/\\/g, '\\\\').replace(/`/g, '\\`')}\``;
}

export function escapeStringLiteral(value: string): string {
  return new SqlEscaper(AnsiSqlDialect).escapeString(String(value));
}
