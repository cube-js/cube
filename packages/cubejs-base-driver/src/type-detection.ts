import { Row, TableStructure } from './driver.interface';

const DB_BIG_INT_MAX = BigInt('9223372036854775807');
const DB_BIG_INT_MIN = BigInt('-9223372036854775808');

const DB_INT_MAX = 2147483647;
const DB_INT_MIN = -2147483648;

const TIMESTAMP_REGEX = /^\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d/;
const DATE_REGEX = /^\d\d\d\d-\d\d-\d\d$/;
const INTEGER_REGEX = /^-?\d+$/;
const DECIMAL_REGEX = /^-?\d+(\.\d+)?$/;

// Matches whole numbers (or their string form) within the inclusive [min, max] range.
const integerRangeMatcher = (min: number | bigint, max: number | bigint) => (v: any): boolean => {
  if (Number.isInteger(v)) {
    return v <= max && v >= min;
  }

  if (INTEGER_REGEX.test(v.toString())) {
    const value = BigInt(v.toString());

    return value <= BigInt(max) && value >= BigInt(min);
  }

  return false;
};

// Order of keys is important here: from more specific to less specific.
// Matchers assume a non-null value: NULL/undefined are filtered out before
// detection (see detectTypesFromTabular), so they never disqualify a type.
const DbTypeValueMatcher: Record<string, ((v: any) => boolean)> = {
  timestamp: (v) => v instanceof Date || TIMESTAMP_REGEX.test(v.toString()),
  date: (v) => v instanceof Date || DATE_REGEX.test(v.toString()),
  int: integerRangeMatcher(DB_INT_MIN, DB_INT_MAX),
  bigint: integerRangeMatcher(DB_BIG_INT_MIN, DB_BIG_INT_MAX),
  decimal: (v) => v instanceof Number || DECIMAL_REGEX.test(v.toString()),
  boolean: (v) => {
    if (v === true || v === false) {
      return true;
    }

    const normalized = v.toString().toLowerCase();

    return normalized === 'true' || normalized === 'false';
  },
  string: (v) => v.length < 256,
  text: () => true
};

const MATCHER_TYPES = Object.keys(DbTypeValueMatcher);

// While detecting column types the first row is normally enough, but when it
// holds NULLs we keep scanning further rows until every column has a concrete
// value to infer its type from. This bounds how many rows we inspect in that case.
const DB_TYPE_DETECTION_MAX_ROWS = 100;

export function detectTypesFromTabular(rows: Row[]): TableStructure {
  if (rows.length === 0) {
    throw new Error(
      'Unable to detect column types for pre-aggregation on empty values in readOnly mode.'
    );
  }

  const fields = Object.keys(rows[0]);

  // Non-null values sampled per column while scanning rows.
  const valuesByField: Record<string, any[]> = {};

  for (const field of fields) {
    valuesByField[field] = [];
  }

  const unresolvedFields = new Set(fields);
  const rowsToScan = Math.min(rows.length, DB_TYPE_DETECTION_MAX_ROWS);

  for (let i = 0; i < rowsToScan; i++) {
    const row = rows[i];

    for (const field of fields) {
      if (field in row && row[field] != null) {
        valuesByField[field].push(row[field]);
        unresolvedFields.delete(field);
      }
    }

    if (unresolvedFields.size === 0) {
      break;
    }
  }

  return fields.map(field => {
    // A column that is NULL across every inspected row has no detectable type.
    if (unresolvedFields.has(field)) {
      return { name: field, type: 'text' };
    }

    const values = valuesByField[field];
    const type = MATCHER_TYPES.find(
      matcherType => values.every(value => DbTypeValueMatcher[matcherType](value))
    ) ?? 'text';

    return { name: field, type };
  });
}
