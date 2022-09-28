import { Readable } from 'stream';
import { Job, QueryResultsOptions } from '@google-cloud/bigquery';

class BigQueryReadStream extends Readable {
    protected nextQuery: QueryResultsOptions | null;

    protected rowsBuffer: any[] = [];

    public constructor(protected job: Job, protected limitPerPage: number) {
      super({
        objectMode: true,
      });
    
      this.nextQuery = {
        autoPaginate: false,
        maxResults: limitPerPage,
      };
    }

    protected manualPaginationCallback(
      err: Error | null,
      rows: any[] | null | undefined,
      nextQuery: QueryResultsOptions | null | undefined,
    ): void {
      if (err) {
        this.destroy(err);
        return;
      }

      this.nextQuery = nextQuery || null;
      this.rowsBuffer.push(...(rows || []));
      this.push(this.rowsBuffer.shift() || null);
    }

    protected readFromBQ(): void {
      if (!this.nextQuery) {
        this.push(null);
        return;
      }
      this.job.getQueryResults(this.nextQuery, this.manualPaginationCallback.bind(this));
    }

    public async _read() {
      if (this.rowsBuffer.length === 0) {
        this.readFromBQ();
      } else {
        this.push(this.rowsBuffer.shift());
      }
    }
}

export const createQueryStreamFromJob = async (job: Job, limitPerPage = 1000) => {
  // Wait for the job to finish.
  await job.getQueryResults({
    maxResults: 0,
  });

  return new BigQueryReadStream(job, limitPerPage);
};
