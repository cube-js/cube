import { Readable } from 'node:stream';
import type { Row } from '@clickhouse/client';

import { buildTransformFromNamesAndTypes, transformRow, type Transform } from './Transform';
import { formatError } from './utils';

type RowBatch = Array<Row<Array<unknown>>>;

/**
 * Streams a JSONCompactEachRowWithNamesAndTypes result set as hydrated row objects.
 *
 * @clickhouse/client hands out batches of rows, so a whole batch is pushed per `await` on the
 * source instead of one row per `await`, and the caller's `highWaterMark` decides how far ahead
 * of the consumer we read.
 */
export class ClickHouseRowStream extends Readable {
  private reading: boolean = false;

  private batch: RowBatch = [];

  private idx: number = 0;

  private transform!: Transform;

  private constructor(
    private readonly source: AsyncIterator<RowBatch>,
    private readonly queryId: string,
    highWaterMark: number,
  ) {
    super({ objectMode: true, highWaterMark });
  }

  /**
   * Consumes the two header rows the format starts with. Reading them here rather than through
   * a consumer keeps it to the one moment where skipping rows is correct.
   */
  public static async open(
    source: AsyncIterator<RowBatch>,
    queryId: string,
    highWaterMark: number,
  ): Promise<{ rowStream: ClickHouseRowStream, names: Array<string>, types: Array<string> }> {
    const rowStream = new ClickHouseRowStream(source, queryId, highWaterMark);

    try {
      const names = await rowStream.readRawRow() as Array<string> | undefined;
      if (!names) {
        throw new Error('Unexpected stream end before row with names');
      }

      const types = await rowStream.readRawRow() as Array<string> | undefined;
      if (!types) {
        throw new Error('Unexpected stream end before row with types');
      }

      rowStream.transform = buildTransformFromNamesAndTypes(names, types);

      return { rowStream, names, types };
    } catch (e) {
      // Nobody holds the stream yet, so destroy it without an error to release the source
      // rather than emit 'error' with no listener attached
      rowStream.destroy();
      throw e;
    }
  }

  /** Advances to the next non-empty batch; false once the source is exhausted. */
  private async fetchBatch(): Promise<boolean> {
    do {
      const next = await this.source.next();
      if (next.done) {
        return false;
      }

      this.batch = next.value;
      this.idx = 0;
    } while (this.batch.length === 0);

    return true;
  }

  private async readRawRow(): Promise<Array<unknown> | undefined> {
    if (this.idx >= this.batch.length && !await this.fetchBatch()) {
      return undefined;
    }

    return this.batch[this.idx++].json();
  }

  public override async _read(): Promise<void> {
    // A synchronous push() can re-enter _read() while the loop below is still running
    if (this.reading) {
      return;
    }
    this.reading = true;

    try {
      let canPush = true;

      while (canPush) {
        if (this.idx >= this.batch.length && !await this.fetchBatch()) {
          this.push(null);
          return;
        }

        canPush = this.push(transformRow(this.batch[this.idx++].json(), this.transform));
      }

      this.reading = false;
    } catch (e) {
      // Since 25.11 the server reports an exception raised after the first block through
      // the response headers and an in-stream tag, and the client rethrows it here.
      this.destroy(new Error(`Stream query failed: ${formatError(e)}; query id: ${this.queryId}`, { cause: e }));
    }
  }

  public override _destroy(error: Error | null, callback: (error?: Error | null) => void): void {
    Promise.resolve(this.source.return?.()).then(() => callback(error), () => callback(error));
  }
}
