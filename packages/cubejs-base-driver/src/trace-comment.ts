import { extractRequestUUID, isValidRequestId } from '@cubejs-backend/shared';

const WHITESPACE = /\s/;

/**
 * The id is emitted verbatim, not sanitized: it has to equal the `trace_id` of
 * the Query History export for the two to join.
 *
 * The API gateway already rejects invalid ids, but this is where the id meets
 * SQL, so the guarantee is checked here too. The allowed alphabet has no `*`,
 * so the id can neither open nor close a block comment.
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

  const withoutSemicolon = sawSemicolon ? sql.slice(0, end) : sql;
  const semicolon = sawSemicolon ? ';' : '';

  return `${withoutSemicolon}\n${comment}${semicolon}`;
}
