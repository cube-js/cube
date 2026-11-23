const INT64_MIN = BigInt('-9223372036854775808');
const INT64_MAX = BigInt('9223372036854775807');

export function isInt64Literal(value: unknown): boolean {
  const str = String(value);
  if (!/^-?\d+$/.test(str)) {
    return false;
  }
  const n = BigInt(str);
  return n >= INT64_MIN && n <= INT64_MAX;
}

export function isNumberLiteral(value: unknown): boolean {
  return /^\s*[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?\s*$/.test(String(value));
}
