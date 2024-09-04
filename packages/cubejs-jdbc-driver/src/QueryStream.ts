import { Readable } from 'stream';

export type Row = {
  [field: string]: boolean | number | string
};

export type nextFn = () => {
  done: boolean,
  value: Row,
};

export function transformRow(row: any) {
  // eslint-disable-next-line no-restricted-syntax
  for (const [name, field] of Object.entries(row)) {
    // console.log({ name, field });
    if (field instanceof Int8Array) {
      row[name] = Buffer.from(field).toString('base64');
    }
  }

  return row;
}

export class QueryStream extends Readable {
  private next: null | nextFn;

  /**
   * @constructor
   */
  public constructor(nextFn: nextFn, highWaterMark: number) {
    super({
      objectMode: true,
      highWaterMark,
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
            this.push(transformRow(row.value));
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
