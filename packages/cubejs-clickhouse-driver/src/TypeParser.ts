/**
 * TODO: replace with ClickHouse's own `@clickhouse/datatype-parser` once this package is ESM. The
 * parser is ESM only, so reaching it from CommonJS needs `await import()`, which turns every parse
 * site async.
 */

export type ParsedType = {
  /** Lowercased type name. */
  name: string,
  /** Top-level arguments, trimmed, in their original case. */
  args: string[],
};

const CHAR_0 = 48;
const CHAR_9 = 57;
const CHAR_LPAREN = 40;
const CHAR_RPAREN = 41;
const CHAR_COMMA = 44;
const CHAR_QUOTE = 39;
const CHAR_BACKSLASH = 92;
const CHAR_BACKTICK = 96;

const NO_ARGS: string[] = [];

function pushArgument(args: string[], type: string, start: number, end: number): void {
  args.push(type.slice(start, end).trim());
}

// `Enum8('a,b' = 1)` must not be split on its comma, and `Map(String, Enum8('a,b' = 1))` must not
// end at the inner parenthesis.
function splitArguments(type: string, start: number): string[] {
  const args: string[] = [];
  let depth = 0;
  let argStart = start;
  let quote = 0;

  for (let i = start; i < type.length; i++) {
    const code = type.charCodeAt(i);

    if (quote !== 0) {
      if (code === CHAR_BACKSLASH) {
        i++;
      } else if (code === quote) {
        quote = 0;
      }
    } else {
      switch (code) {
        case CHAR_QUOTE:
        case CHAR_BACKTICK:
          quote = code;
          break;
        case CHAR_LPAREN:
          depth++;
          break;
        case CHAR_RPAREN:
          if (depth === 0) {
            pushArgument(args, type, argStart, i);

            return args;
          }

          depth--;
          break;
        case CHAR_COMMA:
          if (depth === 0) {
            pushArgument(args, type, argStart, i);
            argStart = i + 1;
          }
          break;
        default:
          break;
      }
    }
  }

  // A type string can arrive from user SQL through `CAST(x AS '…')`, so unbalanced input keeps
  // whatever is there rather than throwing in the middle of a query.
  pushArgument(args, type, argStart, type.length);

  return args;
}

export function parseType(type: string): ParsedType {
  const trimmed = type.trim();
  // A type name holds no quotes or parentheses, so the first parenthesis always opens the arguments.
  const argsStart = trimmed.indexOf('(');

  if (argsStart === -1) {
    return { name: trimmed.toLowerCase(), args: NO_ARGS };
  }

  const args = splitArguments(trimmed, argsStart + 1);

  return {
    name: trimmed.slice(0, argsStart).trim().toLowerCase(),
    // `Foo()` is an empty argument list, not one empty argument.
    args: args.length === 1 && args[0] === '' ? NO_ARGS : args,
  };
}

// Index of the argument the wrapper reads back as. AggregateFunction is absent on purpose: it
// returns an opaque state rather than a value of its argument type.
const TRANSPARENT_WRAPPERS = new Map<string, number>([
  ['nullable', 0],
  ['lowcardinality', 0],
  ['simpleaggregatefunction', 1],
]);

const MAX_UNWRAP_DEPTH = 32;

export function unwrapScalarType(type: string): ParsedType {
  let parsed = parseType(type);

  for (let depth = 0; depth < MAX_UNWRAP_DEPTH; depth++) {
    const argIndex = TRANSPARENT_WRAPPERS.get(parsed.name);

    if (argIndex === undefined || parsed.args.length <= argIndex) {
      return parsed;
    }

    parsed = parseType(parsed.args[argIndex]);
  }

  return parsed;
}

const DEFAULT_DATE_TIME64_PRECISION = 3;
const MAX_DATE_TIME64_PRECISION = 9;

export const PRECISION_UNSUPPORTED = -1;

function parseIntegerArgument(arg: string | undefined): number | null {
  if (arg === undefined || arg.length === 0) {
    return null;
  }

  let value = 0;

  for (let i = 0; i < arg.length; i++) {
    const code = arg.charCodeAt(i);

    if (code < CHAR_0 || code > CHAR_9) {
      return null;
    }

    value = value * 10 + (code - CHAR_0);
  }

  return value;
}

/**
 * Sub-second precision of a DateTime/DateTime64, or PRECISION_UNSUPPORTED for a precision
 * ClickHouse itself does not have. `DateTime` and `DateTime64(0)` read back with the same 19
 * character shape, so a plain DateTime is precision 0.
 */
export function dateTimePrecision(parsed: ParsedType): number {
  if (parsed.name !== 'datetime64') {
    return 0;
  }

  if (parsed.args.length === 0) {
    return DEFAULT_DATE_TIME64_PRECISION;
  }

  const precision = parseIntegerArgument(parsed.args[0]);

  return precision === null || precision > MAX_DATE_TIME64_PRECISION
    ? PRECISION_UNSUPPORTED
    : precision;
}

const NUMERIC_TYPE_NAMES = new Set([
  'int8', 'int16', 'int32', 'int64', 'int128', 'int256',
  'uint8', 'uint16', 'uint32', 'uint64', 'uint128', 'uint256',
  'float32', 'float64', 'bfloat16',
  'decimal', 'decimal32', 'decimal64', 'decimal128', 'decimal256',
]);

export function isNumericTypeName(name: string): boolean {
  return NUMERIC_TYPE_NAMES.has(name);
}

// Types with no scalar equivalent; a value of one is handed over as its JSON rendering. The geo
// types are in here because they are tuples of coordinates.
const OPAQUE_TYPE_NAMES = new Set([
  'map', 'tuple', 'nested', 'variant', 'dynamic', 'json', 'object',
  'point', 'ring', 'polygon', 'multipolygon', 'linestring', 'multilinestring',
]);

export function isOpaqueTypeName(name: string): boolean {
  return OPAQUE_TYPE_NAMES.has(name);
}
