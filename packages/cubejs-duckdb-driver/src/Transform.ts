import {
  blobValue,
  DuckDBDecimalValue,
  DuckDBIntervalValue,
  DuckDBTimeNSValue,
  DuckDBTimeTZValue,
  DuckDBTimeValue,
  DuckDBValue,
  DuckDBValueConverter,
  JS,
  JSDuckDBValueConverter,
  timestampValue,
} from '@duckdb/node-api';

// DuckDB renders DECIMAL with the full declared scale ("100.000"); the legacy driver
// went through a double, so consumers never saw the padding.
function formatDecimal(value: DuckDBDecimalValue): string {
  return value.scale === 0 ? String(value.value) : value.toString().replace(/\.?0+$/, '');
}

/**
 * Converts DuckDB values to the shapes the legacy `duckdb` package produced, at any nesting depth.
 * The default JS converter leaves bigint inside INTERVAL/TIME_TZ/LIST/STRUCT, which JSON cannot
 * serialize, renders TIME as raw microseconds and loses precision on wide DECIMALs.
 */
export const convertDuckDBValue: DuckDBValueConverter<JS> = (value, type, converter) => {
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
  if (converted instanceof Uint8Array && !Buffer.isBuffer(converted)) {
    return Buffer.from(converted.buffer, converted.byteOffset, converted.byteLength);
  }

  return converted;
};

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

export function transformRow(row: any) {
  for (const [field, value] of Object.entries(row)) {
    if (typeof value === 'number' || typeof value === 'bigint') {
      row[field] = value.toString();
    } else if (Object.prototype.toString.call(value) === '[object Date]') {
      row[field] = (value as any).toISOString();
    }
  }
}
