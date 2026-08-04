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

/** Kept at module scope so the trailing-run scan doesn't re-evaluate a literal per character. */
const WHITESPACE = /\s/;

export function sanitizeTraceId(requestId: string | undefined | null): string {
  if (!requestId) {
    return '';
  }

  return String(requestId).replace(ALLOWED_CHARS, '');
}

/**
 * Strips the `-span-` suffix Cube appends per data source query of a request.
 *
 * The emitted id has to match the `trace_id` of the Query History export, which
 * carries the request id without that suffix — joining on the full request id
 * would never match. That makes this the one piece of logic the emitting side
 * and the export side must agree on, so it lives here and the orchestrator
 * re-exports it as `extractRequestUUID` rather than keeping its own copy.
 */
export function toTraceId(requestId: string): string {
  const idx = requestId.lastIndexOf('-span-');

  return idx === -1 ? requestId : requestId.substring(0, idx);
}

export function buildTraceComment(requestId: string | undefined | null): string | null {
  // Strip the span before sanitizing, and cap last. Order is load-bearing in both
  // places. Stripping first keeps the emitted id faithful to the export, which
  // strips the RAW request id: dropping a disallowed character can otherwise close
  // a gap and synthesize a marker that was never there (`myid-span%-1` sanitizes to
  // `myid-span-1`, so the comment would carry `myid` while the export row carries
  // `myid-span%-1`, and the join finds nothing). Sanitizing can never destroy a real
  // marker, since every character of `-span-` is inside the allowlist. Capping last
  // keeps a cap from cutting mid-`-span-` and leaving a partial marker.
  const traceId = sanitizeTraceId(toTraceId(String(requestId ?? ''))).slice(0, MAX_TRACE_ID_LENGTH);
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
