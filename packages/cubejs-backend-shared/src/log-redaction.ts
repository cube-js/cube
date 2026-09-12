/**
 * Redaction of query values in log events: filter values of Cube queries and
 * the parameters of SQL statements (`values`, `params`, `query_values`) are
 * replaced with `redacted` before an event reaches the logger. SQL text is
 * left to its producer: Cube generates it with placeholders, and cubesql
 * redacts the literals of a client statement where it parses it. Literals a
 * data model inlines on purpose (a security context value, a date range) stay.
 *
 * Consumers link the events of one query by an md5 over its queue key
 * (`queryKey`, `cacheKey`, or `sqlQuery.sql`), read from `queryKeyMd5` when the
 * event carries it and computed from the key otherwise. Redacting the key's
 * parameters would change that hash, so `redactLogParams` stamps `queryKeyMd5`
 * over the original key before redacting it.
 */

import crypto from 'crypto';

import type { LoggerFn, LoggerFnParams } from './logger';

export const REDACTED = 'redacted';

/** Head of a schema introspection queue key, kept readable by consumers instead of hashed. */
export const FETCH_TABLES_FOR = 'Fetch tables for';

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

const byteOrder = (a: string, b: string): number => Buffer.compare(Buffer.from(a, 'utf8'), Buffer.from(b, 'utf8'));

const U64_MAX_PLUS_ONE = 2 ** 64;

const I64_MIN = -(2 ** 63);

/**
 * A number as serde_json prints it after parsing what `JSON.stringify` wrote.
 * Integers that fit u64 / i64 round-trip as integers and print the same; the
 * rest parse as f64 and print in the shortest form, where serde_json uses
 * exponent notation below 1e-5 and from 1e16, JavaScript below 1e-6 and from
 * 1e21. Verified against serde_json 1.0.151.
 */
const formatNumber = (value: number): string => {
  if (!Number.isFinite(value)) {
    return 'null';
  }
  const abs = Math.abs(value);
  if (Number.isInteger(value)) {
    return value >= U64_MAX_PLUS_ONE || value < I64_MIN ? value.toExponential() : JSON.stringify(value);
  }
  return abs >= 1e-6 && abs < 1e-5 ? value.toExponential() : JSON.stringify(value);
};

/**
 * The value as compact JSON with object keys in byte order: what
 * `serde_json::to_string` gives a consumer that parsed the logged event, and
 * so what it hashes. Follows `JSON.stringify` otherwise (`toJSON`, dropped
 * `undefined`); returns undefined for a value `JSON.stringify` would omit.
 */
export function canonicalJson(value: unknown): string | undefined {
  if (value === null) {
    return 'null';
  }
  switch (typeof value) {
    case 'string':
    case 'boolean':
      return JSON.stringify(value);
    case 'number':
      return formatNumber(value);
    case 'object': {
      const withToJson = value as { toJSON?: () => unknown };
      if (typeof withToJson.toJSON === 'function') {
        return canonicalJson(withToJson.toJSON());
      }
      if (Array.isArray(value)) {
        return `[${value.map(item => canonicalJson(item) ?? 'null').join(',')}]`;
      }
      const record = value as Record<string, unknown>;
      const members = Object.keys(record)
        .sort(byteOrder)
        .map(key => [key, canonicalJson(record[key])] as const)
        .filter(([, json]) => json !== undefined)
        .map(([key, json]) => `${JSON.stringify(key)}:${json}`);
      return `{${members.join(',')}}`;
    }
    default:
      return undefined;
  }
}

const md5 = (json: string): string => crypto.createHash('md5').update(json).digest('hex');

/** Identity of a queue key: md5 over its canonical JSON, the hash consumers compute from the key in an event. */
export function queryKeyMd5(queryKey: unknown): string {
  return md5(canonicalJson(queryKey) ?? '');
}

/** Whether the key is a schema introspection one: its head, or its first statement, starts with `Fetch tables for`. */
export function isFetchTablesKey(queryKey: unknown): boolean {
  const first = Array.isArray(queryKey) ? queryKey[0] : queryKey;
  const head = Array.isArray(first) ? first[0] : first;
  return typeof head === 'string' && head.startsWith(FETCH_TABLES_FOR);
}

/**
 * The queue key an event identifies its query by, as a consumer derives it:
 * `queryKey`, else `cacheKey`, else the `[sql, params]` of a `sqlQuery` padded
 * to the three elements of a queue key so it hashes like the key the queue
 * logs for the same statement.
 */
function queueKeyOf(params: LoggerFnParams): unknown | undefined {
  if (Array.isArray(params.queryKey)) {
    return params.queryKey;
  }
  if (Array.isArray(params.cacheKey)) {
    return params.cacheKey;
  }
  const sql = isPlainObject(params.sqlQuery) ? params.sqlQuery.sql : undefined;
  if (Array.isArray(sql)) {
    return sql.length < 3 ? [...sql, []] : sql;
  }
  return undefined;
}

/**
 * A copy of log event params with query values redacted: `values` of a filter
 * leaf, statement parameters (`values`, `params`, `query_values`) next to a
 * `query` / `sql` or a `queryKeyMd5`, the params of `[sql, params, ...]`
 * tuples anywhere, and the rows of an inline table. An event carrying a queue key and no
 * `queryKeyMd5` gets one computed over the key before redaction. The input is
 * never mutated.
 */
export function redactLogParams<T extends LoggerFnParams>(params: T): T & { queryKeyMd5?: string } {
  // A class instance would pass through `redactNode` by reference; copy its own properties first
  const source: LoggerFnParams = isPlainObject(params) ? params : { ...(params as LoggerFnParams) };
  const redacted = redactNode(source, new Map()) as T & { queryKeyMd5?: string };

  // Stamped after redaction on purpose: `holdsValues` reads `queryKeyMd5` as the
  // producer's marker of a build payload, not as something this module wrote
  if (redacted.queryKeyMd5 === undefined) {
    const queueKey = queueKeyOf(params);
    const json = isFetchTablesKey(queueKey) ? undefined : canonicalJson(queueKey);
    if (json !== undefined) {
      redacted.queryKeyMd5 = md5(json);
    }
  }

  return redacted;
}

/**
 * Wraps a logger so every event has its query values redacted first. Install
 * it as the outermost wrapper, so that anything reading the params on the way
 * down (telemetry, the agent collector, a custom `logger` option) sees the
 * redacted copy.
 */
export function withLogRedaction(logger: LoggerFn): LoggerFn {
  return (msg, params) => logger(msg, params ? redactLogParams(params) : params);
}
