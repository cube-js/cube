import { extractRequestUUID, isValidRequestId } from '@cubejs-backend/shared';

/**
 * Not sanitized: the id must equal the export's `trace_id`. The `isValidRequestId`
 * alphabet has no `*`, so a valid id cannot break out of the comment.
 */
export function buildTraceComment(requestId: string | undefined | null): string | null {
  const traceId = requestId && extractRequestUUID(requestId);
  if (!traceId || !isValidRequestId(traceId)) {
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
  // comment between two statements, the second empty. Not a regex — `sql` is
  // uncontrolled and `;[\s;]*$` backtracks quadratically on a long run.
  let head = sql.trimEnd();
  if (!head.endsWith(';')) {
    return `${sql}\n${comment}`;
  }

  while (head.endsWith(';')) {
    head = head.slice(0, -1).trimEnd();
  }

  return `${head}\n${comment};`;
}
