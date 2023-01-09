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
  private next: nextFn;

  public constructor(nextFn: nextFn) {
    super({
      objectMode: true,
      highWaterMark: getEnv('dbQueryStreamHighWaterMark'),
    });
    this.next = nextFn;
  }

  public _read(highWaterMark: number): void {
    for (let i = 0; i < highWaterMark; i++) {
      const row = this.next();
      if (row.value) {
        this.push(row.value);
      }
      if (row.done) {
        if (i < highWaterMark) {
          this.push(null);
        }
        break;
      }
    }
  }
}
