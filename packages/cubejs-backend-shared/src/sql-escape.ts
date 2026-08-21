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
}

const AnsiSqlDialect: SqlDialectEscapeRules = {
  stringQuoteChar: '\'',
  doubleQuoteToEscape: true,
  escapeBackslash: false,
  identifierQuoteChar: '"',
};

const MySqlDialect: SqlDialectEscapeRules = {
  stringQuoteChar: '\'',
  doubleQuoteToEscape: true,
  escapeBackslash: true,
  identifierQuoteChar: '`',
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
    return `${q}${String(identifier).split(q).join(q + q)}${q}`;
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
   */
  public format(sql: string, values?: unknown): string {
    if (values === null || values === undefined) {
      return sql;
    }

    const valueList = Array.isArray(values) ? values : [values];

    const placeholders = /\?+/g;

    let result = '';
    let chunkIndex = 0;
    let valuesIndex = 0;
    let placeholderCount = 0;

    let match: RegExpExecArray | null;

    // eslint-disable-next-line no-cond-assign
    while ((match = placeholders.exec(sql)) !== null) {
      // `?` -> value, `??` -> identifier. Longer runs (`???`+) are not placeholders
      // we understand, so we leave them verbatim and consume no value.
      if (match[0].length > 2) {
        continue;
      }

      placeholderCount += 1;

      if (valuesIndex < valueList.length) {
        const rendered = match[0].length === 2
          ? this.escapeIdentifier(String(valueList[valuesIndex]))
          : this.escapeValue(valueList[valuesIndex]);

        result += sql.slice(chunkIndex, match.index) + rendered;
        chunkIndex = placeholders.lastIndex;
        valuesIndex += 1;
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

  protected static parameterCountMismatch(placeholderCount: number, valueCount: number): Error {
    return new Error(
      `SQL parameter count mismatch: ${placeholderCount} placeholder(s) but ${valueCount} value(s) supplied. ` +
      'A literal \'?\' in SQL must be escaped or bound as a parameter.'
    );
  }
}

/**
 * Escaping dialect of the target engine:
 *   - `ansi`  — standard SQL: Presto, Trino, Athena, Dremio, ksqlDB, Pinot, ...
 *   - `mysql` — MySQL / MariaDB: quotes doubled, backslash is an escape char
 *   - `spark` — Spark SQL / Hive / Databricks: backslash escapes only
 */
export type EscapeDialect = 'ansi' | 'mysql' | 'spark';

const Dialects: Record<EscapeDialect, SqlDialectEscapeRules> = {
  ansi: AnsiSqlDialect,
  mysql: MySqlDialect,
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
 * Format a query for Spark SQL / Hive / Databricks: `?` -> escaped value,
 * `??` -> escaped identifier.
 */
export function formatSparkSql(sql: string, values?: unknown): string {
  return format('spark', sql, values);
}

export function escapeStringLiteral(value: string): string {
  return new SqlEscaper(AnsiSqlDialect).escapeString(String(value));
}
