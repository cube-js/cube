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
 *
 * Excluding `+` also keeps the payload from reading as an optimizer hint on
 * Hive and Spark.
 */
const ALLOWED_CHARS = /[^A-Za-z0-9._:-]/g;

const MAX_TRACE_ID_LENGTH = 128;

export function sanitizeTraceId(requestId: string | undefined | null): string {
  if (!requestId) {
    return '';
  }

  return String(requestId).replace(ALLOWED_CHARS, '');
}

/**
 * Strips the `-span-N` suffix Cube appends per data source query of a request.
 *
 * The emitted id has to match the `trace_id` of the Query History export, which
 * carries the request id without that suffix — joining on the full request id
 * would never match.
 */
function toTraceId(requestId: string): string {
  const idx = requestId.lastIndexOf('-span-');

  return idx === -1 ? requestId : requestId.substring(0, idx);
}

export function buildTraceComment(requestId: string | undefined | null): string | null {
  // Cap only after the span is stripped: capping the raw id first can cut it
  // mid-`-span-`, leaving a partial marker in what gets emitted.
  const traceId = toTraceId(sanitizeTraceId(requestId)).slice(0, MAX_TRACE_ID_LENGTH);
  if (!traceId) {
    return null;
  }

  return `/* trace_id: ${traceId} */`;
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
