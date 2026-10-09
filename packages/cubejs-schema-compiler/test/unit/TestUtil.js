import sqlstring from 'sqlstring';

export function debugLog(...args) {
  if (process.env.DEBUG_LOG === 'true') {
    // eslint-disable-next-line
    console.log(...args);
  }
}

export function logSqlAndParams(query) {
  const parts = query.buildSqlAndParams();
  debugLog(sqlstring.format(parts[0], parts[1]));
}
