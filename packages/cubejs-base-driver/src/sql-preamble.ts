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
 * The outcome of scanning a preamble blob for statement boundaries.
 *
 * `ambiguous` is the load-bearing field: callers that only need to execute the
 * statements can treat the whole blob as one and let the engine parse it, but a
 * caller deciding whether the blob is *shaped* a certain way — BigQuery's
 * script-safety guard — must fail closed instead, because a single returned
 * "statement" may in fact contain several.
 */
export type SplitSqlPreambleResult = {
  statements: string[],
  ambiguous: boolean,
};

/**
 * The two dialect rules that change where a statement ends.
 *
 * Neither is knowable from the blob alone, and the drivers that split share this
 * one function, so the scan is run under both and only disagreement counts as
 * ambiguity — see `trySplitSqlPreamble`.
 */
type SqlDialectAssumptions = {
  // MySQL (`NO_BACKSLASH_ESCAPES` off) and BigQuery escape quotes with a
  // backslash; Postgres, DuckDB and Snowflake follow the standard
  // (`standard_conforming_strings`), where a trailing backslash is literal.
  backslashEscapesQuotes: boolean,
  // `#` starts a line comment in MySQL, but is an operator in Postgres (bitwise
  // XOR, geometric ops).
  hashStartsLineComment: boolean,
};

const SQL_DIALECT_ASSUMPTIONS: SqlDialectAssumptions[] = [
  { backslashEscapesQuotes: false, hashStartsLineComment: true },
  { backslashEscapesQuotes: true, hashStartsLineComment: false },
  { backslashEscapesQuotes: true, hashStartsLineComment: true },
  { backslashEscapesQuotes: false, hashStartsLineComment: false },
];

function scanSqlPreamble(normalized: string, dialect: SqlDialectAssumptions): SplitSqlPreambleResult {
  const statements: string[] = [];
  let current = '';
  let index = 0;
  // Tracks whether anything executable — not whitespace, not a comment — has
  // been consumed since the last separator. Comments are kept in the statement
  // text (a driver may rely on a hint inside one), but a segment holding nothing
  // else is not a statement: Snowflake rejects an empty statement outright, and
  // BigQuery's script-safety guard would refuse a legitimate preamble that
  // simply ends with a comment.
  let executable = false;
  // Set when the scan meets something it cannot interpret at all — an
  // unterminated literal, comment or dollar-quoted body. Splitting on a guess
  // would hand a data source a fragment of the user's SQL, so the whole blob is
  // returned as one statement instead and the engine's own parser decides.
  let ambiguous = false;

  // Finds the end of a quoted string or identifier, or -1 when it never closes.
  //
  // A doubled quote is an escaped quote in every dialect here; whether a
  // backslash is one depends on the dialect.
  const endOfQuoted = (start: number, quote: string): number => {
    let cursor = start + 1;

    while (cursor < normalized.length) {
      if (normalized[cursor] !== quote) {
        cursor += 1;
      } else if (normalized[cursor + 1] === quote) {
        cursor += 2;
      } else if (dialect.backslashEscapesQuotes && normalized[cursor - 1] === '\\') {
        cursor += 1;
      } else {
        return cursor + 1;
      }
    }

    return -1;
  };

  // Finds the end of a block comment, honouring nesting — Postgres, DuckDB and
  // Snowflake all nest them. Returns -1 when it never closes.
  const endOfBlockComment = (start: number): number => {
    let depth = 0;
    let cursor = start;

    while (cursor < normalized.length - 1) {
      if (normalized.startsWith('/*', cursor)) {
        depth += 1;
        cursor += 2;
      } else if (normalized.startsWith('*/', cursor)) {
        depth -= 1;
        cursor += 2;

        if (depth === 0) {
          return cursor;
        }
      } else {
        cursor += 1;
      }
    }

    return -1;
  };

  while (index < normalized.length && !ambiguous) {
    const char = normalized[index];
    // Dollar-quoted body, e.g. $$ ... $$ or $tag$ ... $tag$ (Postgres UDFs).
    // Anchored at `index` so a `$` elsewhere in an identifier is not an opener.
    const dollarTag = /^\$[A-Za-z_0-9]*\$/.exec(normalized.slice(index));
    let end: number;

    if (normalized.startsWith('--', index) || (char === '#' && dialect.hashStartsLineComment)) {
      // Line comment — runs to the end of the line.
      const newline = normalized.indexOf('\n', index);
      end = newline === -1 ? normalized.length : newline;
    } else if (normalized.startsWith('/*', index)) {
      end = endOfBlockComment(index);
    } else if (dollarTag) {
      const tag = dollarTag[0];
      const close = normalized.indexOf(tag, index + tag.length);
      end = close === -1 ? -1 : close + tag.length;
    } else if (char === '\'' || char === '"' || char === '`') {
      end = endOfQuoted(index, char);
      executable = true;
    } else if (char === ';') {
      const statement = normalizeSqlPreamble(current);
      if (statement && executable) {
        statements.push(statement);
      }
      current = '';
      executable = false;
      index += 1;
      end = -2;
    } else {
      current += char;
      executable = executable || !/\s/.test(char);
      index += 1;
      end = -2;
    }

    if (end === -1) {
      ambiguous = true;
    } else if (end !== -2) {
      current += normalized.slice(index, end);
      index = end;
    }
  }

  if (ambiguous) {
    return { statements: [normalized], ambiguous: true };
  }

  const last = normalizeSqlPreamble(current);
  if (last && executable) {
    statements.push(last);
  }

  return { statements, ambiguous: false };
}

function sameStatements(a: SplitSqlPreambleResult, b: SplitSqlPreambleResult): boolean {
  return !a.ambiguous
    && a.statements.length === b.statements.length
    && a.statements.every((statement, i) => statement === b.statements[i]);
}

/**
 * Splits a preamble blob back into statements, reporting whether the boundaries
 * could be determined confidently.
 *
 * Semicolons inside string literals, dollar-quoted bodies and comments do not
 * separate statements, so this is a real scan rather than a `split(';')` —
 * splitting naively would tear apart any preamble defining a UDF, which is the
 * feature's main use case.
 *
 * Two of the rules the scan needs are dialect-dependent (see
 * `SqlDialectAssumptions`) and the drivers that split share this one function, so
 * the blob is scanned under each combination. Agreement means the dialect does
 * not matter for this input, which is the overwhelmingly common case; a
 * disagreement is real ambiguity and yields the blob whole.
 *
 * Prefer `splitSqlPreamble` when you just need statements to execute; use this
 * when ambiguity must change the decision.
 */
export function trySplitSqlPreamble(preamble?: string | null): SplitSqlPreambleResult {
  const normalized = normalizeSqlPreamble(preamble);

  if (!normalized) {
    return { statements: [], ambiguous: false };
  }

  const [first, ...rest] = SQL_DIALECT_ASSUMPTIONS.map(dialect => scanSqlPreamble(normalized, dialect));

  if (first.ambiguous || rest.some(other => !sameStatements(other, first))) {
    return { statements: [normalized], ambiguous: true };
  }

  return first;
}

/**
 * Splits a preamble blob into statements for drivers whose execution API takes
 * one statement at a time (JDBC, Snowflake, DuckDB).
 *
 * An ambiguous blob comes back as a single entry, which is the conservative
 * choice for execution: the data source's own parser is authoritative. Callers
 * that must not treat an unparsed blob as one statement should use
 * `trySplitSqlPreamble` and check `ambiguous`.
 */
export function splitSqlPreamble(preamble?: string | null): string[] {
  return trySplitSqlPreamble(preamble).statements;
}

/**
 * True for the error a data source raises when a preamble statement has already
 * taken effect on this connection.
 *
 * Drivers that pool and reuse connections run the preamble on each acquire,
 * because a pooled connection is not guaranteed to be one the preamble has
 * already run on. That makes re-execution normal rather than exceptional, and a
 * `CREATE …` statement — the feature's main use case — is not idempotent. There
 * is no way to tell a creating statement from a session-setting one without
 * parsing SQL, so an already-applied statement is treated as satisfied.
 *
 * Deliberately narrow: only "already exists" and its per-engine spellings are
 * tolerated, so a genuine syntax or permission error still surfaces.
 */
export function isAlreadyAppliedPreambleError(e: unknown): boolean {
  const message = (e as Error)?.message ?? '';

  return /already exists|duplicate function|duplicate table|duplicate key name|is already defined/i.test(message);
}

/**
 * Runs the preamble one statement at a time, skipping statements that a
 * previous run on this connection has already applied.
 *
 * Takes either a blob to split or an already-split statement list, so a caller
 * that has separated the preamble from other statements does not have to re-join
 * and re-parse it.
 */
export async function applySqlPreambleStatements(
  preamble: string | string[] | undefined | null,
  execute: (statement: string) => Promise<unknown>,
): Promise<void> {
  const statements = Array.isArray(preamble) ? preamble : splitSqlPreamble(preamble);

  for (const statement of statements) {
    try {
      await execute(statement);
    } catch (e) {
      if (!isAlreadyAppliedPreambleError(e)) {
        throw e;
      }
    }
  }
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
 * Resolves the configured preamble: the `sqlPreamble` option if set, else the
 * environment value.
 *
 * A blank option falls through to the environment rather than suppressing it —
 * `sqlPreamble: process.env.MY_PREAMBLE || ''` is an easy thing to template into
 * a config, and it should not silently disable `CUBEJS_DB_SQL_PREAMBLE`.
 *
 * Deprecated aliases are NOT handled here: `initSql` and
 * `prepareConnectionQueries` differ from `sqlPreamble` in failure posture and in
 * how they combine with a driver's built-in connection queries, so each owning
 * driver resolves its own alias.
 */
export function resolveSqlPreamble(
  options: { sqlPreamble?: string | null },
  fromEnv?: string | null,
): string | undefined {
  return normalizeSqlPreamble(options.sqlPreamble) ?? normalizeSqlPreamble(fromEnv);
}
