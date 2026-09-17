import { Readable } from 'stream';
import { DuckDBResult } from '@duckdb/node-api';

import { Transform, transformChunk } from './Transform';

/**
 * Streams a DuckDB result as hydrated row objects, one fetched chunk (up to 2048 rows) per
 * `await` instead of one row per generator step. The consumer's `highWaterMark` bounds how far
 * ahead of it the chunks are read.
 */
export class DuckDBRowStream extends Readable {
  private reading = false;

  private pendingFetch: Promise<unknown> | null = null;

  private batch: Record<string, unknown>[] = [];

  private idx = 0;

  public constructor(
    private readonly result: DuckDBResult,
    private readonly transform: Transform,
    private readonly onClose: () => void,
    highWaterMark: number,
  ) {
    super({ objectMode: true, highWaterMark });
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
        if (this.idx >= this.batch.length) {
          const fetch = this.result.fetchChunk();
          this.pendingFetch = fetch;
          const chunk = await fetch.finally(() => {
            this.pendingFetch = null;
          });

          // Nobody reads the rows a destroy() raced with
          if (this.destroyed) {
            return;
          }

          if (chunk === null || chunk.rowCount === 0) {
            this.push(null);
            return;
          }

          this.batch = transformChunk(chunk, this.transform);
          this.idx = 0;
        }

        canPush = this.push(this.batch[this.idx++]);
      }
    } catch (e) {
      this.destroy(e as Error);
    } finally {
      this.reading = false;
    }
  }

  public override _destroy(error: Error | null, callback: (error?: Error | null) => void): void {
    const close = () => {
      try {
        this.onClose();
        callback(error);
      } catch (e) {
        // Letting this escape the handler would leave the stream without a 'close' event and
        // raise an unhandled rejection; the destroy error, when there is one, came first.
        callback(error || e as Error);
      }
    };

    // The connection must not close under an in-flight native fetch
    Promise.resolve(this.pendingFetch).then(close, close);
  }
}
