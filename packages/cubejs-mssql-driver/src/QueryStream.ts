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
  public constructor(request: sql.Request, highWaterMark: number) {
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
    });
    this.request.on('done', () => {
      this.push(null);
    });
    this.request.on('error', (err: Error) => {
      this.destroy(err);
    });
  }

  /**
   * @override
   */
  public _read(toRead: number) {
    this.toRead += toRead;
    this.request?.resume();
  }

  private transformRow(row: Record<string, any>) {
    for (const [key, value] of Object.entries(row)) {
      if (value instanceof Date) {
        row[key] = value.toJSON();
      }
    }
  }

  /**
   * @override
   */
  public _destroy(error: any, callback: CallableFunction) {
    this.request?.cancel();
    this.request = null;
    callback(error);
  }
}
