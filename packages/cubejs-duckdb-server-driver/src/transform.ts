export function transformValue(value: any) {
  if (typeof value === 'number' || typeof value === 'bigint') {
    return value.toString();
  } else if (Object.prototype.toString.call(value) === '[object Date]') {
    return (value as any).toISOString();
  }
  return value;
}
