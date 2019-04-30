const sqlstring = require('sqlstring');

exports.logSqlAndParams = function(query) {
    let parts = query.buildSqlAndParams()
    // debugLog(parts[0]);
    // debugLog(parts[1]);
    exports.debugLog(sqlstring.format(parts[0], parts[1]));
}
  
exports.debugLog = function() {
    if (process.env.DEBUG_LOG === 'true') {
        console.log.apply(console, [...arguments])
    }
}
  