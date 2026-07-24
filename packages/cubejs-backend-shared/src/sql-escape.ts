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
   *   - `?`  is replaced by an escaped value ({@link escapeValue})
   *   - `??` is replaced by an escaped identifier ({@link escapeIdentifier})
   * Longer runs of `?` are left untouched. This mirrors `sqlstring.format`'s
   * placeholder semantics so it can be a drop-in replacement in drivers.
   */
  public format(sql: string, values?: unknown): string {
    if (values === null || values === undefined) {
      return sql;
    }

    const valueList = Array.isArray(values) ? values : [values];
    if (valueList.length === 0) {
      return sql;
    }

    const placeholders = /\?+/g;
    let result = '';
    let chunkIndex = 0;
    let valuesIndex = 0;
    let match: RegExpExecArray | null = placeholders.exec(sql);

    while (valuesIndex < valueList.length && match !== null) {
      const len = match[0].length;
      // `?` -> value, `??` -> identifier. Longer runs (`???`+) are not placeholders
      // we understand, so we leave them verbatim and consume no value.
      if (len <= 2) {
        const rendered = len === 2
          ? this.escapeIdentifier(String(valueList[valuesIndex]))
          : this.escapeValue(valueList[valuesIndex]);

        result += sql.slice(chunkIndex, match.index) + rendered;
        chunkIndex = placeholders.lastIndex;
        valuesIndex += 1;
      }
      match = placeholders.exec(sql);
    }

    if (chunkIndex === 0) {
      return sql;
    }
    return chunkIndex < sql.length ? result + sql.slice(chunkIndex) : result;
  }
}

/**
 * Format a query for standard-SQL engines (Presto, Trino, Postgres, ...):
 * `?` -> escaped value, `??` -> escaped identifier.
 */
export function formatAnsi(sql: string, values?: unknown): string {
  return new SqlEscaper(AnsiSqlDialect).format(sql, values);
}

/**
 * Format a query for MySQL / MariaDB: `?` -> escaped value, `??` -> escaped
 * identifier.
 */
export function formatMySql(sql: string, values?: unknown): string {
  return new SqlEscaper(MySqlDialect).format(sql, values);
}
