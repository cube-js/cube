import { Readable } from 'stream';

export type Row = {
  [field: string]: boolean | number | string | null
};

/**
 * Resolves to the next batch of rows, or to null once the result set is exhausted.
 */
export type NextBatchFn = () => Promise<Row[] | null>;

export class QueryStream extends Readable {
  protected nextBatch: null | NextBatchFn;

  protected reading: boolean = false;

  /**
   * @constructor
   */
  public constructor(nextBatch: NextBatchFn, highWaterMark: number) {
    super({
      objectMode: true,
      highWaterMark,
    });
    this.nextBatch = nextBatch;
  }

  /**
   * @override
   */
  public _read(): void {
    // A batch is fetched on a JVM worker thread and the result set is not thread-safe.
    if (this.reading || !this.nextBatch) {
      return;
    }

    this.reading = true;
    this.nextBatch().then(
      (rows) => {
        this.reading = false;

        if (!this.nextBatch) {
          return;
        }

        if (!rows) {
          this.push(null);
          return;
        }

        if (rows.length === 0) {
          this._read();
          return;
        }

        for (let i = 0; i < rows.length; i++) {
          this.push(rows[i]);
        }
      },
      (err) => {
        this.reading = false;
        this.destroy(err);
      }
    );
  }

  /**
   * @override
   */
  public _destroy(
    error: Error | null,
    callback: (error?: Error | null | undefined) => void,
  ): void {
    this.nextBatch = null;
    callback(error);
  }
}
