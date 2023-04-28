const { Readable } = require('stream');
const { getEnv } = require('@cubejs-backend/shared');

/**
 * MS-SQL query stream class.
 */
class QueryStream extends Readable {
  request = null;
  recordset = [];
  done = false;

  /**
   * @constructor
   */
  constructor(request, highWaterMark) {
    super({
      objectMode: true,
      highWaterMark:
        highWaterMark || getEnv('dbQueryStreamHighWaterMark'),
    });
    this.request = request;
    this.request.on('row', (row) => {
      this.recordset?.push(row);
      if (this.recordset?.length === getEnv('dbQueryStreamHighWaterMark')) {
        this.request.pause();
      }
    });
    this.request.on('error', (err) => {
      this.destroy(err);
    });
    this.request.on('done', () => {
      this.done = true;
    });
  }

  /**
   * @override
   */
  _read(highWaterMark) {
    const chunk = this.recordset?.splice(0, highWaterMark);
    chunk?.forEach((row) => {
      this.push(row);
    });
    if (this.recordset?.length === 0 && this.done) {
      this.push(null);
    } else {
      this.request.resume();
    }
  }

  /**
   * @override
   */
  _destroy(error, callback) {
    this.request = null;
    this.recordset = null;
    callback(error);
  }
}

module.exports = QueryStream;
