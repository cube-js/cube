/* eslint-disable import/no-extraneous-dependencies */
import { Readable } from 'stream';
import { getEnv } from '@cubejs-backend/shared';

export type Row = {
  [field: string]: boolean | number | string
};

export type nextFn = () => {
  done: boolean,
  value: Row,
};

export class QueryStream extends Readable {
  private next: null | nextFn;

  /**
   * @constructor
   */
  public constructor(nextFn: nextFn) {
    super({
      objectMode: true,
      highWaterMark: getEnv('dbQueryStreamHighWaterMark'),
    });
    this.next = nextFn;
  }

  /**
   * @override
   */
  public _read(highWaterMark: number): void {
    setTimeout(() => {
      for (let i = 0; i < highWaterMark; i++) {
        if (this.next) {
          const row = this.next();
          if (row.value) {
            this.push(row.value);
          }
          if (row.done) {
            this.push(null);
            break;
          }
        }
      }
    }, 0);
  }

  /**
   * @override
   */
  public _destroy(
    error: Error | null,
    callback: (error?: Error | null | undefined) => void,
  ): void {
    this.next = null;
    callback(error);
  }
}
