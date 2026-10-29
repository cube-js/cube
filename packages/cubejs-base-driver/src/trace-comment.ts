import { extractRequestUUID, isValidRequestId } from '@cubejs-backend/shared';

const WHITESPACE = /\s/;

/**
 * Emitted verbatim (minus `-span-`) to match the export's `trace_id`; ids failing
 * `isValidRequestId` are dropped — its alphabet has no `*`, so no comment breakout.
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
 * Appended, not prepended: Snowflake strips leading comments before recording the SQL text.
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
