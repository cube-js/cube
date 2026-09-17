import { buildObjectShape } from '@cubejs-backend/shared';
import { DateTime } from 'luxon';
import {
  blobValue,
  DuckDBBlobValue,
  DuckDBDataChunk,
  DuckDBDateValue,
  DuckDBDecimalValue,
  DuckDBIntervalValue,
  DuckDBTimestampMillisecondsValue,
  DuckDBTimestampNanosecondsValue,
  DuckDBTimestampSecondsValue,
  DuckDBTimestampTZValue,
  DuckDBTimestampValue,
  DuckDBType,
  DuckDBTypeId,
  DuckDBValue,
  DuckDBValueConverter,
  JS,
  JSDuckDBValueConverter,
  timestampValue,
} from '@duckdb/node-api';

/** Never receives null; the caller short-circuits those. */
export type ColumnConverter = (value: DuckDBValue) => unknown;

export type Transform = {
  names: string[],
  converters: Array<ColumnConverter | null>,
  objectShape: Record<string, unknown> | null,
};

const MILLIS_PER_DAY = 86400000;

/**
 * Renders epoch millis as an ISO timestamp, matching `new Date(millis).toISOString()`
 * byte for byte across the whole range Date accepts, expanded years included.
 */
export function formatIsoFromMillis(millis: number): string {
  const iso = DateTime.fromMillis(millis, { zone: 'utc' }).toISO();
  if (iso) {
    return iso;
  }

  // luxon returns null exactly where Date throws: NaN and anything outside
  // -271821-04-20 .. +275760-09-13 (|millis| > 8.64e15).
  throw new RangeError(`Invalid time value: ${millis}`);
}

function formatDecimal(value: DuckDBDecimalValue): string {
  if (value.scale === 0) {
    return String(value.value);
  }

  const text = value.toString();

  // `/\.?0+$/` matches with no point at all, which would eat the integer part of "100"
  return text.includes('.') ? text.replace(/\.?0+$/, '') : text;
}

function toBuffer(bytes: Uint8Array): Buffer {
  return Buffer.isBuffer(bytes) ? bytes : Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength);
}

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

/**
 * Recursive converter for nested types (LIST, STRUCT, MAP, ...), where the legacy driver
 * kept JS built-ins. Nested cells go through the very same converters as top-level columns,
 * so a TIMESTAMP inside a STRUCT is the same ISO string as a TIMESTAMP column.
 */
export const convertNestedValue: DuckDBValueConverter<JS> = (value, type, converter) => {
  if (value === null) {
    return null;
  }

  const columnConverter = CONVERTERS_BY_TYPE_ID[type.typeId];
  if (columnConverter !== undefined) {
    return (columnConverter === null ? value : columnConverter(value)) as JS;
  }

  return JSDuckDBValueConverter(value, type, converter);
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
 * Column-major: each vector is decoded once and its converter is picked once, instead of
 * dispatching on the type for every cell.
 */
export function transformChunk(chunk: DuckDBDataChunk, transform: Transform): Record<string, unknown>[] {
  const { names, converters, objectShape } = transform;
  const { rowCount } = chunk;
  const rows: Record<string, unknown>[] = new Array(rowCount);

  for (let rowIndex = 0; rowIndex < rowCount; rowIndex++) {
    // A prototype-less row keeps a `__proto__` column a plain cell; `{}` would hand the
    // assignment below to Object.prototype's setter, which drops it and can swap the row's
    // prototype. The shape clone is immune by construction.
    rows[rowIndex] = objectShape === null ? Object.create(null) : { ...objectShape };
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
