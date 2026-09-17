import { buildObjectShape } from '@cubejs-backend/shared';
import {
  blobValue,
  DuckDBBlobValue,
  DuckDBDataChunk,
  DuckDBDateValue,
  DuckDBDecimalValue,
  DuckDBIntervalValue,
  DuckDBTimeNSValue,
  DuckDBTimestampMillisecondsValue,
  DuckDBTimestampNanosecondsValue,
  DuckDBTimestampSecondsValue,
  DuckDBTimestampTZValue,
  DuckDBTimestampValue,
  DuckDBTimeTZValue,
  DuckDBTimeValue,
  DuckDBType,
  DuckDBTypeId,
  DuckDBValue,
  DuckDBValueConverter,
  JS,
  JSDuckDBValueConverter,
  timestampValue,
} from '@duckdb/node-api';

/** Converts one non-null column value into what Cube expects to see in a row. */
export type ColumnConverter = (value: DuckDBValue) => unknown;

export type Transform = {
  names: string[],
  converters: Array<ColumnConverter | null>,
  objectShape: Record<string, unknown> | null,
};

const MILLIS_PER_DAY = 86400000;

const MIN_FAST_ISO_MILLIS = -62167219200000; // 0000-01-01T00:00:00.000Z
const MAX_FAST_ISO_MILLIS = 253402300799999; // 9999-12-31T23:59:59.999Z

const PAD2: string[] = Array.from({ length: 100 }, (_, i) => `${i}`.padStart(2, '0'));
const PAD3: string[] = Array.from({ length: 1000 }, (_, i) => `${i}`.padStart(3, '0'));

/**
 * `new Date(millis).toISOString()` without the Date allocation and the parse/format round trip.
 * Only for the four-digit-year range; outside it (and for NaN) Date decides, including its
 * RangeError for out-of-range values.
 */
export function formatIsoFromMillis(millis: number): string {
  if (!(millis >= MIN_FAST_ISO_MILLIS && millis <= MAX_FAST_ISO_MILLIS)) {
    return new Date(millis).toISOString();
  }

  const days = Math.floor(millis / MILLIS_PER_DAY);
  let rest = millis - days * MILLIS_PER_DAY;
  const ms = rest % 1000;
  rest = (rest - ms) / 1000;
  const second = rest % 60;
  rest = (rest - second) / 60;
  const minute = rest % 60;
  const hour = (rest - minute) / 60;

  // civil_from_days, http://howardhinnant.github.io/date_algorithms.html
  const z = days + 719468;
  const era = Math.floor(z / 146097);
  const doe = z - era * 146097;
  const yoe = Math.floor((doe - Math.floor(doe / 1460) + Math.floor(doe / 36524) - Math.floor(doe / 146096)) / 365);
  const doy = doe - (365 * yoe + Math.floor(yoe / 4) - Math.floor(yoe / 100));
  const mp = Math.floor((5 * doy + 2) / 153);
  const day = doy - Math.floor((153 * mp + 2) / 5) + 1;
  const month = mp < 10 ? mp + 3 : mp - 9;
  const year = yoe + era * 400 + (month <= 2 ? 1 : 0);

  const yyyy = year < 1000 ? `${year}`.padStart(4, '0') : `${year}`;

  return `${yyyy}-${PAD2[month]}-${PAD2[day]}T${PAD2[hour]}:${PAD2[minute]}:${PAD2[second]}.${PAD3[ms]}Z`;
}

// DuckDB renders DECIMAL with the full declared scale ("100.000"); the legacy driver
// went through a double, so consumers never saw the padding.
function formatDecimal(value: DuckDBDecimalValue): string {
  return value.scale === 0 ? String(value.value) : value.toString().replace(/\.?0+$/, '');
}

function toBuffer(bytes: Uint8Array): Buffer {
  return Buffer.isBuffer(bytes) ? bytes : Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength);
}

/**
 * Recursive converter for nested types (LIST, STRUCT, MAP, ...), where the legacy driver
 * kept JS built-ins. The default JS converter leaves bigint that JSON cannot serialize,
 * renders TIME as raw microseconds and loses precision on wide DECIMALs.
 */
export const convertNestedValue: DuckDBValueConverter<JS> = (value, type, converter) => {
  if (value === null) {
    return null;
  }

  if (typeof value === 'bigint') {
    return value.toString();
  }

  if (value instanceof DuckDBDecimalValue) {
    return formatDecimal(value);
  }

  if (value instanceof DuckDBTimeValue || value instanceof DuckDBTimeNSValue || value instanceof DuckDBTimeTZValue) {
    return value.toString();
  }

  if (value instanceof DuckDBIntervalValue) {
    return { months: value.months, days: value.days, micros: Number(value.micros) };
  }

  const converted = JSDuckDBValueConverter(value, type, converter);

  return converted instanceof Uint8Array ? toBuffer(converted) : converted;
};

const stringConverter: ColumnConverter = (value) => String(value);

const decimalConverter: ColumnConverter = (value) => formatDecimal(value as DuckDBDecimalValue);

const intervalConverter: ColumnConverter = (value) => {
  const { months, days, micros } = value as DuckDBIntervalValue;

  return { months, days, micros: Number(micros) };
};

const blobConverter: ColumnConverter = (value) => toBuffer((value as DuckDBBlobValue).bytes);

const dateConverter: ColumnConverter = (value) => formatIsoFromMillis((value as DuckDBDateValue).days * MILLIS_PER_DAY);

// bigint division truncates toward zero like the JS converter, so sub-millisecond digits
// of pre-epoch values round the same way they always did.
const timestampConverter: ColumnConverter = (value) => formatIsoFromMillis(
  Number((value as DuckDBTimestampValue | DuckDBTimestampTZValue).micros / 1000n)
);

const timestampSecondsConverter: ColumnConverter = (value) => formatIsoFromMillis(
  Number((value as DuckDBTimestampSecondsValue).seconds) * 1000
);

const timestampMillisConverter: ColumnConverter = (value) => formatIsoFromMillis(
  Number((value as DuckDBTimestampMillisecondsValue).millis)
);

const timestampNanosConverter: ColumnConverter = (value) => formatIsoFromMillis(
  Number((value as DuckDBTimestampNanosecondsValue).nanos / 1000000n)
);

// The legacy driver returned JS built-ins and a second pass stringified top-level numbers
// and formatted Dates as ISO; each converter here does both steps in one call.
const CONVERTERS_BY_TYPE_ID: Partial<Record<DuckDBTypeId, ColumnConverter | null>> = {
  [DuckDBTypeId.BOOLEAN]: null,
  [DuckDBTypeId.VARCHAR]: null,
  [DuckDBTypeId.SQLNULL]: null,
  [DuckDBTypeId.TINYINT]: stringConverter,
  [DuckDBTypeId.SMALLINT]: stringConverter,
  [DuckDBTypeId.INTEGER]: stringConverter,
  [DuckDBTypeId.UTINYINT]: stringConverter,
  [DuckDBTypeId.USMALLINT]: stringConverter,
  [DuckDBTypeId.UINTEGER]: stringConverter,
  [DuckDBTypeId.FLOAT]: stringConverter,
  [DuckDBTypeId.DOUBLE]: stringConverter,
  [DuckDBTypeId.BIGINT]: stringConverter,
  [DuckDBTypeId.UBIGINT]: stringConverter,
  [DuckDBTypeId.HUGEINT]: stringConverter,
  [DuckDBTypeId.UHUGEINT]: stringConverter,
  [DuckDBTypeId.BIGNUM]: stringConverter,
  [DuckDBTypeId.ENUM]: stringConverter,
  [DuckDBTypeId.UUID]: stringConverter,
  [DuckDBTypeId.TIME]: stringConverter,
  [DuckDBTypeId.TIME_TZ]: stringConverter,
  [DuckDBTypeId.TIME_NS]: stringConverter,
  [DuckDBTypeId.DECIMAL]: decimalConverter,
  [DuckDBTypeId.INTERVAL]: intervalConverter,
  [DuckDBTypeId.BLOB]: blobConverter,
  [DuckDBTypeId.DATE]: dateConverter,
  [DuckDBTypeId.TIMESTAMP]: timestampConverter,
  [DuckDBTypeId.TIMESTAMP_TZ]: timestampConverter,
  [DuckDBTypeId.TIMESTAMP_S]: timestampSecondsConverter,
  [DuckDBTypeId.TIMESTAMP_MS]: timestampMillisConverter,
  [DuckDBTypeId.TIMESTAMP_NS]: timestampNanosConverter,
};

export function getColumnConverter(type: DuckDBType): ColumnConverter | null {
  const converter = CONVERTERS_BY_TYPE_ID[type.typeId];
  if (converter !== undefined) {
    return converter;
  }

  return (value) => convertNestedValue(value, type, convertNestedValue);
}

export function buildTransform(names: string[], types: DuckDBType[]): Transform {
  if (names.length !== types.length) {
    throw new Error(`Unexpected names and types length mismatch; names ${names.length} vs types ${types.length}`);
  }

  return {
    names,
    converters: types.map(getColumnConverter),
    objectShape: buildObjectShape(names),
  };
}

/**
 * Hydrates every row of a chunk. Column-major: each vector is decoded once and the converter
 * for it is picked once, instead of dispatching on the type for every cell.
 */
export function transformChunk(chunk: DuckDBDataChunk, transform: Transform): Record<string, unknown>[] {
  const { names, converters, objectShape } = transform;
  const { rowCount } = chunk;
  const rows: Record<string, unknown>[] = new Array(rowCount);

  for (let rowIndex = 0; rowIndex < rowCount; rowIndex++) {
    rows[rowIndex] = objectShape === null ? {} : { ...objectShape };
  }

  for (let columnIndex = 0; columnIndex < names.length; columnIndex++) {
    const name = names[columnIndex];
    const converter = converters[columnIndex];
    const vector = chunk.getColumnVector(columnIndex);

    if (converter === null) {
      for (let rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        rows[rowIndex][name] = vector.getItem(rowIndex);
      }
    } else {
      for (let rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        const value = vector.getItem(rowIndex);
        rows[rowIndex][name] = value === null ? null : converter(value);
      }
    }
  }

  return rows;
}

export function convertDuckDBParams(values: unknown[] | null | undefined): DuckDBValue[] {
  return (values || []).map((value) => {
    if (value instanceof Date) {
      return timestampValue(BigInt(value.getTime()) * 1000n);
    }

    if (value instanceof Uint8Array) {
      return blobValue(value);
    }

    return value as DuckDBValue;
  });
}
