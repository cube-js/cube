const { Readable } = require('stream');
const { getEnv } = require('@cubejs-backend/shared');

/**
 * MS-SQL query stream class.
 */
class QueryStream extends Readable {
  request = null;
  toRead = 0;

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
    this.request.on('row', row => {
      this.transformRow(row);
      const canAdd = this.push(row);
      if (this.toRead-- <= 0 || !canAdd) {
        this.request.pause();
      }
    })
    this.request.on('done', () => {
      this.push(null);
    })
    this.request.on('error', (err) => {
      this.destroy(err);
    });
  }

  /**
   * @override
   */
  _read(toRead) {
    this.toRead += toRead;
    this.request.resume();
  }

  transformRow(row) {
    for (const key in row) {
      if (row.hasOwnProperty(key) && row[key] && row[key] instanceof Date) {
        row[key] = row[key].toJSON();
      }
    }
  }

  /**
   * @override
   */
  _destroy(error, callback) {
    this.request.cancel();
    this.request = null;
    callback(error);
  }
}

module.exports = QueryStream;
