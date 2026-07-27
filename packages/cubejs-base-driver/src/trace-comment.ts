/**
 * Attaches a `trace_id` SQL comment to queries sent to data sources, so that a
 * warehouse's query history can be joined back to Cube's own query history.
 *
 * The request id reaching this module can be supplied by the client — the REST
 * API reads `x-request-id`/`traceparent` verbatim — so it is sanitized with an
 * allowlist rather than by escaping known-bad sequences. Both comment
 * delimiters have to go: engines are split on whether block comments nest, so a
 * stray closing delimiter leaks live SQL on some, and a stray opening one
 * swallows the statement on others.
 */

/**
 * Characters kept in a trace id. Covers every id shape Cube produces or
 * accepts: UUIDs, `-span-N` suffixes, W3C `traceparent`, and the `scheduler-`
 * and `datasources-` prefixes.
 */
const ALLOWED_CHARS = /[^A-Za-z0-9._:-]/g;

const MAX_TRACE_ID_LENGTH = 128;

export function sanitizeTraceId(requestId: string | undefined | null): string {
  if (!requestId) {
    return '';
  }

  return String(requestId)
    .replace(ALLOWED_CHARS, '')
    // A payload starting with `+` reads as an optimizer hint on Hive and Spark.
    .replace(/^\++/, '')
    .slice(0, MAX_TRACE_ID_LENGTH);
}

/**
 * Splits a request id into the trace id and the span that produced this
 * particular query.
 *
 * The trace id must match the `trace_id` of the Query History export, which
 * drops the `-span-N` suffix — joining on the full request id would never
 * match. The span is emitted alongside it, since one request fans out into
 * several data source queries that are otherwise indistinguishable.
 */
function splitRequestId(requestId: string): { traceId: string, span?: string } {
  const idx = requestId.lastIndexOf('-span-');
  if (idx === -1) {
    return { traceId: requestId };
  }

  return {
    traceId: requestId.substring(0, idx),
    span: requestId.substring(idx + '-span-'.length),
  };
}

export function buildTraceComment(requestId: string | undefined | null): string | null {
  const sanitized = sanitizeTraceId(requestId);
  if (!sanitized) {
    return null;
  }

  const { traceId, span } = splitRequestId(sanitized);
  if (!traceId) {
    return null;
  }

  return span
    ? `/* trace_id: ${traceId} span: ${span} */`
    : `/* trace_id: ${traceId} */`;
}

/**
 * Returns `sql` with the trace comment appended, or unchanged when the request
 * id has nothing usable left after sanitization.
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

  const withoutSemicolon = sql.replace(/;\s*$/, '');
  const semicolon = withoutSemicolon.length === sql.length ? '' : ';';

  return `${withoutSemicolon}\n${comment}${semicolon}`;
}
