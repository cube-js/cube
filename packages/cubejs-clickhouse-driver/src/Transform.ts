import * as moment from 'moment';

export type ColumnConverter = (value: unknown) => unknown;

export type ColumnMeta = { name: string, type: string };

export type Transform = {
  names: Array<string>,
  converters: Array<ColumnConverter | null>,
  nullPrototype: boolean,
};

const CHAR_0 = 48;
const CHAR_9 = 57;
const CHAR_SPACE = 32;
const CHAR_DASH = 45;
const CHAR_DOT = 46;
const CHAR_COLON = 58;
const CHAR_T = 84;
const CHAR_Z = 90;

const ZEROS = '000';

function isDigit(code: number): boolean {
  return code >= CHAR_0 && code <= CHAR_9;
}

function hasCanonicalDateTimePrefix(s: string): boolean {
  if (s.length < 19) {
    return false;
  }

  const separator = s.charCodeAt(10);
  if (
    s.charCodeAt(4) !== CHAR_DASH ||
    s.charCodeAt(7) !== CHAR_DASH ||
    (separator !== CHAR_SPACE && separator !== CHAR_T) ||
    s.charCodeAt(13) !== CHAR_COLON ||
    s.charCodeAt(16) !== CHAR_COLON
  ) {
    return false;
  }

  return isDigit(s.charCodeAt(0)) && isDigit(s.charCodeAt(1)) &&
    isDigit(s.charCodeAt(2)) && isDigit(s.charCodeAt(3)) &&
    isDigit(s.charCodeAt(5)) && isDigit(s.charCodeAt(6)) &&
    isDigit(s.charCodeAt(8)) && isDigit(s.charCodeAt(9)) &&
    isDigit(s.charCodeAt(11)) && isDigit(s.charCodeAt(12)) &&
    isDigit(s.charCodeAt(14)) && isDigit(s.charCodeAt(15)) &&
    isDigit(s.charCodeAt(17)) && isDigit(s.charCodeAt(18));
}

// The server can override `date_time_output_format`, so only bypass moment for known-equivalent
// simple and ISO values.
export function formatCanonicalDateTime(s: string): string | null {
  if (!hasCanonicalDateTimePrefix(s)) {
    return null;
  }

  const len = s.length;
  let millis = ZEROS;
  let tail = 19;

  if (len > 19 && s.charCodeAt(19) === CHAR_DOT) {
    let end = 20;
    while (end < len && isDigit(s.charCodeAt(end))) {
      end++;
    }

    const fractionLength = end - 20;
    if (fractionLength === 0) {
      return null;
    }

    millis = fractionLength >= 3
      ? s.slice(20, 23)
      : s.slice(20, end) + ZEROS.slice(fractionLength);

    tail = end;
  }

  // Offsets require a timezone shift and stay on the moment fallback path.
  if (tail !== len && !(tail === len - 1 && s.charCodeAt(tail) === CHAR_Z)) {
    return null;
  }

  return `${s.slice(0, 10)}T${s.slice(11, 19)}.${millis}`;
}

export function formatDateTime(value: unknown): string {
  if (typeof value === 'string') {
    const formatted = formatCanonicalDateTime(value);
    if (formatted !== null) {
      return formatted;
    }
  }

  return moment.utc(value as any).format(moment.HTML5_FMT.DATETIME_LOCAL_MS);
}

const dateTimeConverter: ColumnConverter = (value) => (
  value === null || value === undefined ? value : formatDateTime(value)
);

const dateConverter: ColumnConverter = (value) => (
  value === null || value === undefined ? value : `${value}T00:00:00.000`
);

const numberConverter: ColumnConverter = (value) => {
  if (value === null || value === undefined) {
    return value;
  }

  return typeof value === 'string' ? value : String(value);
};

const WRAPPER_PREFIXES = ['Nullable(', 'LowCardinality('];

// Scalar names inside container arguments must not select a converter for the container itself.
// SimpleAggregateFunction is excluded because it reads back as its scalar argument type.
const NON_SCALAR_PREFIXES = ['Array(', 'Map(', 'Tuple(', 'Nested(', 'Enum', 'JSON', 'AggregateFunction('];

function unwrapScalar(type: string): string {
  let inner = type;
  let stripped = true;

  while (stripped) {
    stripped = false;

    for (const prefix of WRAPPER_PREFIXES) {
      if (inner.startsWith(prefix)) {
        inner = inner.slice(prefix.length);
        stripped = true;
      }
    }
  }

  return inner;
}

export function getColumnConverter(type: string): ColumnConverter | null {
  const inner = unwrapScalar(type);

  if (NON_SCALAR_PREFIXES.some((prefix) => inner.startsWith(prefix))) {
    return null;
  }

  if (inner.includes('Date')) {
    return inner.includes('DateTime') ? dateTimeConverter : dateConverter;
  }

  if (inner.includes('Int') || inner.includes('Float') || inner.includes('Decimal')) {
    return numberConverter;
  }

  return null;
}

function buildTransform(names: Array<string>, converters: Array<ColumnConverter | null>): Transform {
  return {
    names,
    converters,
    // Assignment to `__proto__` invokes Object.prototype's setter. Limit the slower null-prototype
    // object to result sets that need it.
    nullPrototype: names.includes('__proto__'),
  };
}

export function buildTransformFromMeta(meta: ReadonlyArray<ColumnMeta>): Transform {
  const names: Array<string> = new Array(meta.length);
  const converters: Array<ColumnConverter | null> = new Array(meta.length);

  for (let i = 0; i < meta.length; i++) {
    names[i] = meta[i].name;
    converters[i] = getColumnConverter(meta[i].type);
  }

  return buildTransform(names, converters);
}

export function buildTransformFromNamesAndTypes(names: Array<string>, types: Array<string>): Transform {
  if (names.length !== types.length) {
    throw new Error(`Unexpected names and types length mismatch; names ${names.length} vs types ${types.length}`);
  }

  const converters: Array<ColumnConverter | null> = new Array(names.length);
  for (let i = 0; i < names.length; i++) {
    converters[i] = getColumnConverter(types[i]);
  }

  return buildTransform(names, converters);
}

// Left-to-right assignment preserves JSON.parse's ordering and last-value behavior for duplicate
// column names, keeping pre-aggregation content versions stable.
export function transformRow(row: ReadonlyArray<unknown>, transform: Transform): Record<string, unknown> {
  const { names, converters } = transform;

  if (row.length !== names.length) {
    throw new Error(`Unexpected row and names/types length mismatch; row ${row.length} vs names ${names.length}`);
  }

  const rowObj: Record<string, unknown> = transform.nullPrototype ? Object.create(null) : {};

  for (let i = 0; i < names.length; i++) {
    const converter = converters[i];
    const value = row[i];
    rowObj[names[i]] = converter === null ? value : converter(value);
  }

  return rowObj;
}
