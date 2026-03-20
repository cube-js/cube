import type { DuckDBValue, DuckDBType, DuckDBValueConverter } from '@duckdb/node-api';

/**
 * Custom DuckDB value converter that converts all values to Cube-friendly formats:
 * - Dates/timestamps → ISO 8601 strings
 * - Decimals → strings with trailing zeros trimmed
 * - Bigints → strings
 * - Numbers → strings (for consistency)
 */
export const cubeValueConverter: DuckDBValueConverter<string | null> = (value: DuckDBValue, _type: DuckDBType) => {
  if (value === null || value === undefined) {
    return null;
  }

  // Handle bigint
  if (typeof value === 'bigint') {
    return value.toString();
  }

  // Handle number
  if (typeof value === 'number') {
    return value.toString();
  }

  // Handle Date objects (from timestamps/dates)
  if (value instanceof Date) {
    return value.toISOString();
  }

  // Handle DuckDB value objects
  const valueObj = value as any;
  const constructorName = valueObj?.constructor?.name;

  if (constructorName === 'DuckDBTimestampValue' && 'micros' in valueObj) {
    // Convert microseconds since epoch to ISO string
    const micros = BigInt(valueObj.micros);
    const millis = Number(micros / 1000n);
    return new Date(millis).toISOString();
  }

  if (constructorName === 'DuckDBDateValue' && 'days' in valueObj) {
    // Convert days since epoch to ISO string
    const days = Number(valueObj.days);
    const millis = days * 24 * 60 * 60 * 1000;
    return new Date(millis).toISOString();
  }

  if (constructorName === 'DuckDBDecimalValue' && 'value' in valueObj && 'scale' in valueObj) {
    // Convert decimal to string with proper formatting
    const bigintValue = BigInt(valueObj.value);
    const scale = Number(valueObj.scale);

    if (scale === 0) {
      return bigintValue.toString();
    }

    // Format decimal value with the fractional part
    const valueStr = bigintValue.toString();
    const isNegative = valueStr[0] === '-';
    const absValueStr = isNegative ? valueStr.slice(1) : valueStr;
    const paddedValue = absValueStr.padStart(scale + 1, '0');
    const integerPart = paddedValue.slice(0, -scale) || '0';
    const fractionalPart = paddedValue.slice(-scale);
    const formattedValue = `${isNegative ? '-' : ''}${integerPart}.${fractionalPart}`;
    // Remove trailing zeros and decimal point if no fractional part remains
    return formattedValue.replace(/\.?0+$/, '') || '0';
  }

  // Fallback: convert to string
  return String(value);
};
