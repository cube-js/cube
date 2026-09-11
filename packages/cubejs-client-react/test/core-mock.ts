export function isQueryPresent(query: unknown) {
  return Boolean(query && Object.keys(query).length);
}

export function areQueriesEqual(left: unknown, right: unknown) {
  return JSON.stringify(left) === JSON.stringify(right);
}
