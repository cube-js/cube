export function uniqArray<T = any>(array: T[]) {
  return Array.from(new Set(array));
}
