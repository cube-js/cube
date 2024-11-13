export function debugLog() {
  if (process.env.DEBUG_LOG === 'true') {
    // eslint-disable-next-line
    console.log.apply(console, [...arguments]);
  }
}

export function logSqlAndParams(query) {
  const parts = query.buildSqlAndParams();
  debugLog(parts[0]);
  debugLog(parts[1]);
}
