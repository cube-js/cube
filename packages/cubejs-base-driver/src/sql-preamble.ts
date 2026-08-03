import type { LoggerFn } from '@cubejs-backend/shared';

/**
 * SQL that runs in the context of every query on a data source connection —
 * Cube's counterpart to Looker's `sql_preamble`, typically used to define
 * temporary UDFs or set session parameters.
 *
 * The value is one opaque blob rather than a parsed statement list: commas and
 * semicolons both occur inside SQL literals, so no delimiter could be imposed
 * without an escaping convention. Drivers that need individual statements split
 * with `splitSqlPreamble`; the rest hand the blob to the data source, whose own
 * parser is authoritative.
 */

/**
 * Trims a preamble and collapses blank values to undefined, so `''` and
 * whitespace-only input behave as "not configured" rather than producing an
 * empty statement.
 */
export function normalizeSqlPreamble(preamble?: string | null): string | undefined {
  if (typeof preamble !== 'string') {
    return undefined;
  }

  const trimmed = preamble.trim();

  return trimmed.length ? trimmed : undefined;
}

/**
 * Joins the legacy `prepareConnectionQueries: string[]` shape into a single
 * blob. Statements are separated by `;\n` and any trailing separator on an
 * individual entry is dropped first, so re-joining an already-terminated list
 * cannot produce empty statements.
 */
export function joinSqlPreamble(preamble?: string | string[] | null): string | undefined {
  if (Array.isArray(preamble)) {
    const statements = preamble
      .map(statement => normalizeSqlPreamble(statement))
      .filter((statement): statement is string => statement !== undefined)
      .map(statement => statement.replace(/;\s*$/, ''));

    return normalizeSqlPreamble(statements.join(';\n'));
  }

  return normalizeSqlPreamble(preamble);
}

/**
 * Splits a preamble blob back into statements for drivers whose execution API
 * takes one statement at a time (JDBC).
 *
 * Semicolons inside string literals, dollar-quoted bodies and comments do not
 * separate statements, so this is a real scan rather than a `split(';')` —
 * splitting naively would tear apart any preamble defining a UDF, which is the
 * feature's main use case.
 */
export function splitSqlPreamble(preamble?: string | null): string[] {
  const normalized = normalizeSqlPreamble(preamble);

  if (!normalized) {
    return [];
  }

  const statements: string[] = [];
  let current = '';
  let index = 0;

  // Finds the end of a quoted string or identifier. A doubled quote is an
  // escaped quote rather than a terminator, and a backslash escape is honoured
  // too since MySQL uses them.
  const endOfQuoted = (start: number, quote: string): number => {
    let cursor = start + 1;

    while (cursor < normalized.length) {
      if (normalized[cursor] === '\\') {
        cursor += 2;
      } else if (normalized[cursor] !== quote) {
        cursor += 1;
      } else if (normalized[cursor + 1] === quote) {
        cursor += 2;
      } else {
        return cursor + 1;
      }
    }

    return cursor;
  };

  while (index < normalized.length) {
    const char = normalized[index];
    const rest = normalized.slice(index);
    // Dollar-quoted body, e.g. $$ ... $$ or $tag$ ... $tag$ (Postgres UDFs).
    const dollarTag = /^\$[A-Za-z_0-9]*\$/.exec(rest);
    let end: number;

    if (rest.startsWith('--')) {
      // Line comment — runs to the end of the line.
      const newline = normalized.indexOf('\n', index);
      end = newline === -1 ? normalized.length : newline;
    } else if (rest.startsWith('/*')) {
      // Block comment — nesting is dialect-specific, so the first `*/` ends it.
      const close = normalized.indexOf('*/', index + 2);
      end = close === -1 ? normalized.length : close + 2;
    } else if (dollarTag) {
      const tag = dollarTag[0];
      const close = normalized.indexOf(tag, index + tag.length);
      end = close === -1 ? normalized.length : close + tag.length;
    } else if (char === '\'' || char === '"' || char === '`') {
      end = endOfQuoted(index, char);
    } else if (char === ';') {
      const statement = normalizeSqlPreamble(current);
      if (statement) {
        statements.push(statement);
      }
      current = '';
      index += 1;
      end = -1;
    } else {
      current += char;
      index += 1;
      end = -1;
    }

    if (end !== -1) {
      current += normalized.slice(index, end);
      index = end;
    }
  }

  const last = normalizeSqlPreamble(current);
  if (last) {
    statements.push(last);
  }

  return statements;
}

/**
 * Prepends the preamble to a query for stateless data sources (BigQuery),
 * where the preamble is only meaningful inside the same request as the
 * primary query — a BigQuery temporary UDF exists for one query only.
 */
export function prependSqlPreamble(query: string, preamble?: string | null): string {
  const normalized = normalizeSqlPreamble(preamble);

  if (!normalized) {
    return query;
  }

  const terminated = /;\s*$/.test(normalized) ? normalized : `${normalized};`;

  return `${terminated}\n${query}`;
}

/**
 * Resolves the preamble from the new option and its deprecated aliases,
 * warning once per driver instance when a legacy name is used.
 *
 * `sqlPreamble` wins when several are set, so adopting the new name is never
 * silently overridden by a leftover legacy value. Precedence is fixed rather
 * than merged: concatenating an old and a new preamble would run statements the
 * user never asked to combine.
 */
export function resolveSqlPreamble(
  options: {
    sqlPreamble?: string | null,
    /** Deprecated alias, DuckDB. */
    initSql?: string | null,
    /** Deprecated alias, JDBC-family. Accepts the legacy array shape. */
    prepareConnectionQueries?: string | string[] | null,
  },
  logger?: LoggerFn,
): string | undefined {
  const preamble = normalizeSqlPreamble(options.sqlPreamble);

  if (preamble) {
    return preamble;
  }

  const deprecated: [string, string | undefined][] = [
    ['initSql', normalizeSqlPreamble(options.initSql)],
    ['prepareConnectionQueries', joinSqlPreamble(options.prepareConnectionQueries)],
  ];

  for (const [name, value] of deprecated) {
    if (value) {
      logger?.('Deprecated driver option', {
        warning: `The ${name} driver option is deprecated and will be removed in a future release. Use sqlPreamble instead.`,
      });

      return value;
    }
  }

  return undefined;
}
