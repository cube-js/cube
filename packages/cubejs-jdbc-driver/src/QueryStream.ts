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

  /**
   * The batch being fetched on a JVM worker thread. The connection can't be reused until it settles.
   */
  protected pending: Promise<void> | null = null;

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
    if (this.pending || !this.nextBatch) {
      return;
    }

    this.pending = this.nextBatch().then(
      (rows) => {
        this.pending = null;

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
        this.pending = null;
        this.destroy(err);
      }
    );
  }

  /**
   * Destroys the stream and resolves once no batch is being fetched, so the result set
   * and its connection can be closed or reused.
   */
  public async close(): Promise<void> {
    this.destroy();

    if (this.pending) {
      await this.pending;
    }
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
