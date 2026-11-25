import { Readable } from 'stream';

export type PinotRow = Record<string, unknown>;

/**
 * Resolves with the next page of rows, or `null` once the result is exhausted.
 */
export type PinotPageReader = () => Promise<PinotRow[] | null>;

export class PinotRowStream extends Readable {
  protected reading = false;

  public constructor(
    protected readonly nextPage: PinotPageReader,
    highWaterMark: number,
  ) {
    super({ objectMode: true, highWaterMark });
  }

  public override _read(): void {
    if (this.reading) {
      return;
    }

    this.reading = true;

    this.readPages().then(
      () => {
        this.reading = false;
      },
      (error) => {
        this.reading = false;
        this.destroy(error);
      }
    );
  }

  protected async readPages(): Promise<void> {
    let wantsMore = true;

    while (wantsMore && !this.destroyed) {
      const rows = await this.nextPage();
      if (this.destroyed) {
        return;
      }

      if (!rows) {
        this.push(null);
        return;
      }

      for (const row of rows) {
        wantsMore = this.push(row);
      }
    }
  }
}
