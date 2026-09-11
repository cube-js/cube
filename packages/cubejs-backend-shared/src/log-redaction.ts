/**
 * Redaction of query values for the log sink. SQL text is left to its
 * producer: Cube generates it with placeholders, and cubesql attaches a
 * redacted twin (`redactedQuery`, `redactedError`) beside a client statement,
 * which the sink swaps in. Literals a data model inlines on purpose (a security
 * context value, a date range) stay.
 */

import type { LoggerFn, LoggerFnParams } from './logger';

export const REDACTED = 'redacted';

const isPlainObject = (value: unknown): value is Record<string, unknown> => {
  if (typeof value !== 'object' || value === null) {
    return false;
  }
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
};

/** `[sql, params, ...]`: `sqlQuery.sql`, a pre-aggregation `loadSql`, the queue's `queryKey` / `cacheKey`. */
const isSqlWithParams = (value: unknown[]): value is [string, unknown[], ...unknown[]] => typeof value[0] === 'string' && Array.isArray(value[1]);

/** Element by element, so the arity of an `IN` filter or a parameter list survives. */
const redactValues = (value: unknown): unknown => (
  Array.isArray(value) ? value.map(() => REDACTED) : REDACTED
);

/** `params` and `query_values` only ever hold statement parameters; `values` is gated below. */
const PARAMETER_KEYS = new Set(['values', 'params', 'query_values']);

/**
 * Whether `values` here holds statement values: a filter leaf (`member` /
 * `dimension`), a statement (`query` / `sql`), or a pre-aggregation build,
 * which carries `queryKeyMd5` and no SQL.
 */
const holdsValues = (node: Record<string, unknown>): boolean => (
  'member' in node || 'dimension' in node || 'query' in node || 'sql' in node || 'queryKeyMd5' in node
);

function redactNode(node: unknown, memo: Map<object, unknown>): unknown {
  if (Array.isArray(node)) {
    const seen = memo.get(node);
    if (seen !== undefined) {
      return seen;
    }
    const out: unknown[] = [];
    memo.set(node, out);

    if (isSqlWithParams(node)) {
      const [sql, params, ...rest] = node;
      out.push(sql, params.map(() => REDACTED));
      for (const item of rest) {
        out.push(redactNode(item, memo));
      }
      return out;
    }

    for (const item of node) {
      out.push(redactNode(item, memo));
    }
    return out;
  }

  if (isPlainObject(node)) {
    const seen = memo.get(node);
    if (seen !== undefined) {
      return seen;
    }
    const out: Record<string, unknown> = {};
    memo.set(node, out);

    const redactValuesKey = holdsValues(node);

    for (const [key, value] of Object.entries(node)) {
      const isParameters = PARAMETER_KEYS.has(key) && (key !== 'values' || redactValuesKey);
      // An inline table (`name`, `columns`, `csvRows`) carries data source rows
      const isInlineRows = key === 'csvRows' && 'columns' in node;
      out[key] = isParameters || isInlineRows ? redactValues(value) : redactNode(value, memo);
    }
    return out;
  }

  // Primitives, and instances of anything but a plain object (Error, Date, streams)
  return node;
}

/** Redacted twins cubesql attaches beside the fields they replace in the log sink. */
const COMPANIONS: ReadonlyArray<[companion: string, target: string]> = [
  ['redactedQuery', 'query'],
  ['redactedError', 'error'],
];

/**
 * A copy of log event params with query values redacted: `values` of a filter
 * leaf, statement parameters (`values`, `params`, `query_values`) next to a
 * `query` / `sql` or a `queryKeyMd5`, the params of `[sql, params, ...]`
 * tuples anywhere, the rows of an inline table, and `query` / `error` replaced
 * by the redacted twin cubesql attached. The input is never mutated.
 */
export function redactLogParams<T extends LoggerFnParams>(params: T): T {
  // A class instance would pass through `redactNode` by reference; copy its own properties first
  const source: LoggerFnParams = isPlainObject(params) ? params : { ...(params as LoggerFnParams) };
  const redacted = redactNode(source, new Map()) as Record<string, unknown>;

  for (const [companion, target] of COMPANIONS) {
    if (companion in redacted) {
      redacted[target] = redacted[companion];
      delete redacted[companion];
    }
  }

  // A `query.sql` with no twin beside it is a SQL API statement (or whatever a
  // client sent in its place) that its producer did not redact; it is dropped
  // rather than written as received.
  const { query } = redacted;
  if (!('redactedQuery' in params) && isPlainObject(query) && 'sql' in query) {
    redacted.query = { ...query, sql: REDACTED };
  }

  return redacted as T;
}

/**
 * Wraps the log sink so every event it writes has its query values redacted.
 * Wrap the sink itself, not the wrappers around it: the agent collector and
 * telemetry forward the original event, and must keep doing so.
 */
export function withLogRedaction(logger: LoggerFn): LoggerFn {
  return (msg, params) => logger(msg, params ? redactLogParams(params) : params);
}
