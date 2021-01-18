import sqlstring from 'sqlstring';

export function logSqlAndParams(query) {
  const parts = query.buildSqlAndParams();
  // debugLog(parts[0]);
  // debugLog(parts[1]);
  exports.debugLog(sqlstring.format(parts[0], parts[1]));
}

export function debugLog() {
  if (process.env.DEBUG_LOG === 'true') {
    // eslint-disable-next-line
    console.log.apply(console, [...arguments]);
  }
}
