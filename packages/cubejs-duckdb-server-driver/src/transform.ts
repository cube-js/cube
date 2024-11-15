import { DataType, Field } from 'apache-arrow';

export function transformValue(field: Field, value: any) {
  if (DataType.isDate(field.type) || DataType.isTimestamp(field.type)) {
    return new Date(value).toISOString();
  } else if (typeof value === 'number' || typeof value === 'bigint') {
    return value.toString();
  }
  return value;
}
