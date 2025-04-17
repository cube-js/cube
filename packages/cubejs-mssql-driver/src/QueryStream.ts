import { Readable } from 'stream';
import sql from 'mssql';
import {
  getEnv,
} from '@cubejs-backend/shared';

/**
 * MS-SQL query stream class.
 */
export class QueryStream extends Readable {
  private request: sql.Request | null;
  private toRead: number = 0;

  /**
   * @constructor
   */
  constructor(request: sql.Request, highWaterMark: number) {
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
        this.request?.pause();
      }
    })
    this.request.on('done', () => {
      this.push(null);
    })
    this.request.on('error', (err: Error) => {
      this.destroy(err);
    });
  }

  /**
   * @override
   */
  _read(toRead: number) {
    this.toRead += toRead;
    this.request?.resume();
  }

  transformRow(row: Record<string, any>) {
    for (const key in row) {
      if (row.hasOwnProperty(key) && row[key] && row[key] instanceof Date) {
        row[key] = row[key].toJSON();
      }
    }
  }

  /**
   * @override
   */
  _destroy(error: any, callback: CallableFunction) {
    this.request?.cancel();
    this.request = null;
    callback(error);
  }
}
