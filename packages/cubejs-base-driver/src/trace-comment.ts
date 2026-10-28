import { extractRequestUUID, isValidRequestId } from '@cubejs-backend/shared';

/**
 * Attaches a `trace_id` SQL comment to queries sent to data sources, so that a
 * warehouse's query history can be joined back to Cube's own query history.
 */

/** Kept at module scope so the trailing-run scan doesn't re-evaluate a literal per character. */
const WHITESPACE = /\s/;

/**
 * The emitted id has to match the `trace_id` of the Query History export, which
 * is the request id without its `-span-` suffix, so it is used verbatim rather
 * than sanitized: dropping characters would break that join.
 *
 * The id is spliced into SQL text, so one failing `isValidRequestId` is not
 * emitted at all. The API gateway already rejects such ids from clients; the
 * check keeps the guarantee next to the sink rather than in another package.
 * The allowed alphabet has no `*`, so the id can neither open nor close a block
 * comment.
 */
export function buildTraceComment(requestId: string | undefined | null): string | null {
  if (!requestId || !isValidRequestId(requestId)) {
    return null;
  }

  const traceId = extractRequestUUID(requestId);
  if (!traceId) {
    return null;
  }

  return `/* trace_id: ${traceId} */`;
}

/**
 * Returns `sql` with the trace comment appended, or unchanged when there is no
 * valid request id.
 *
 * Appended rather than prepended because Snowflake strips leading comments
 * before recording the SQL text, and no data source requires a leading one.
 * The comment goes before any trailing semicolon, which some clients treat as
 * a statement separator.
 */
export function addTraceComment(sql: string, requestId: string | undefined | null): string {
  const comment = buildTraceComment(requestId);
  if (!comment) {
    return sql;
  }

  // Take the whole trailing run, not one semicolon: a leftover `;` would leave the
  // comment between two statements, the second empty. Scanned, not matched — `sql`
  // is uncontrolled and `;[\s;]*$` backtracks quadratically on a long run.
  let end = sql.length;
  let sawSemicolon = false;
  while (end > 0) {
    const ch = sql[end - 1];
    if (ch === ';') {
      sawSemicolon = true;
    } else if (!WHITESPACE.test(ch)) {
      break;
    }
    end -= 1;
  }

  // Trailing whitespace on its own must not gain the query a semicolon it never had.
  const withoutSemicolon = sawSemicolon ? sql.slice(0, end) : sql;
  const semicolon = sawSemicolon ? ';' : '';

  return `${withoutSemicolon}\n${comment}${semicolon}`;
}
