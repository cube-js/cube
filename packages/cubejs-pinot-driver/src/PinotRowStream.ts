import { Readable } from 'stream';

export type PinotRow = Record<string, unknown>;

/**
 * Resolves with the next page of rows, or `null` once the result is exhausted.
 */
export type PinotPageReader = () => Promise<PinotRow[] | null>;

/**
 * Called once when the stream is closed, whether it ended, failed or was destroyed.
 */
export type PinotStreamCloser = () => Promise<void>;

export class PinotRowStream extends Readable {
  protected reading = false;

  protected readRequested = false;

  public constructor(
    protected readonly nextPage: PinotPageReader,
    highWaterMark: number,
    protected readonly close?: PinotStreamCloser,
  ) {
    super({ objectMode: true, highWaterMark });
  }

  public override _read(): void {
    // A request that arrives while a page is being pushed is remembered instead of dropped:
    // Node does not repeat it, so losing it would stall the stream.
    this.readRequested = true;

    if (this.reading) {
      return;
    }

    this.reading = true;

    this.readPages().catch((error) => {
      this.destroy(error);
    });
  }

  public override _destroy(error: Error | null, callback: (error?: Error | null) => void): void {
    if (!this.close) {
      callback(error);
      return;
    }

    this.close().then(
      () => callback(error),
      (closeError) => callback(error || closeError)
    );
  }

  protected async readPages(): Promise<void> {
    try {
      while (this.readRequested && !this.destroyed) {
        this.readRequested = false;

        const rows = await this.nextPage();
        if (this.destroyed) {
          return;
        }

        if (!rows) {
          this.push(null);
          return;
        }

        let wantsMore = true;

        for (const row of rows) {
          if (this.destroyed) {
            return;
          }

          wantsMore = this.push(row);
        }

        if (wantsMore) {
          this.readRequested = true;
        }
      }
    } finally {
      // Reset in the same tick the loop exits in, so that no `_read()` can fall in between.
      this.reading = false;
    }
  }
}
