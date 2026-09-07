import Axios, { AxiosRequestConfig } from 'axios';
import { streamToArray } from '@cubejs-backend/shared';
import readline from 'node:readline';
import { Readable } from 'node:stream';

export type DruidClientStreamResult = {
  rowStream: Readable,
  columns: Record<string, { sqlType: string }> | null,
  release: () => Promise<void>,
};

export type DruidClientBaseConfiguration = {
  user?: string,
  password?: string,
  database?: string,
};

export type DruidClientConfiguration = DruidClientBaseConfiguration & {
  url: string,
};

export class DruidClient {
  public constructor(
    protected readonly config: DruidClientConfiguration,
  ) {
  }

  protected getClient() {
    const config: AxiosRequestConfig = {
      baseURL: this.config.url,
      headers: {
        'Content-Type': 'application/json',
      }
    };

    if (this.config.user && this.config.password) {
      config.auth = {
        username: this.config.user,
        password: this.config.password,
      };
    }

    return Axios.create(config);
  }

  public async cancel(queryId: string) {
    return this.getClient().request({
      url: `/druid/v2/${queryId}`,
      method: 'DELETE',
    });
  }

  public async query<R = unknown>(query: string, parameters: { type: string, value: unknown }[]): Promise<{ rows: R[], columns: Record<string, { sqlType: string }> | null }> {
    let cancelled = false;
    const cancelObj: any = {};

    const promise: Promise<{ rows: R[], columns: any }> & { cancel?: () => void } = (async () => {
      cancelObj.cancel = async () => {
        cancelled = true;
      };

      try {
        const response = await this.getClient().request({
          url: '/druid/v2/sql/',
          method: 'POST',
          data: {
            query,
            parameters,
            header: true,
            sqlTypesHeader: true,
            resultFormat: 'object',
          },
        });

        if (cancelled && response.headers['x-druid-sql-query-id']) {
          await this.cancel(response.headers['x-druid-sql-query-id']);

          throw new Error('Query cancelled');
        }

        if (response.headers['x-druid-sql-header-included']) {
          const [columns, ...rows] = response.data;

          return {
            columns,
            rows
          };
        } else {
          return {
            columns: null,
            rows: response.data,
          };
        }
      } catch (e: any) {
        if (cancelled) {
          throw new Error('Query cancelled');
        }

        if (e.response && e.response.data) {
          if (e.response.data.errorMessage) {
            throw new Error(e.response.data.errorMessage);
          }
        }

        throw e;
      }
    })();

    promise.cancel = () => cancelObj.cancel();
    return promise;
  }

  public async stream(
    query: string,
    parameters: { type: string, value: unknown }[],
    highWaterMark: number,
  ): Promise<DruidClientStreamResult> {
    let response;

    try {
      response = await this.getClient().request({
        url: '/druid/v2/sql/',
        method: 'POST',
        responseType: 'stream',
        data: {
          query,
          parameters,
          header: true,
          sqlTypesHeader: true,
          resultFormat: 'objectLines',
        },
      });
    } catch (e: any) {
      // In stream mode Druid returns the error as a streamed JSON body,
      // so we need to drain it to surface the original errorMessage.
      if (e.response && e.response.data instanceof Readable) {
        const body = (await streamToArray<Buffer>(e.response.data))
          .map((chunk) => chunk.toString('utf-8'))
          .join('');

        let errorMessage: string | undefined;
        try {
          errorMessage = JSON.parse(body)?.errorMessage;
        } catch {
          // Body wasn't valid JSON, fall back to the original error below.
        }

        if (errorMessage) {
          throw new Error(errorMessage);
        }
      }

      throw e;
    }

    const httpStream: Readable = response.data;
    const rl = readline.createInterface({ input: httpStream, crlfDelay: Infinity });
    const lineIterator = rl[Symbol.asyncIterator]();

    const release = async () => {
      rl.close();
      httpStream.destroy();
    };

    try {
      // The first non-empty line is the header (column name -> { sqlType }),
      // matching the non-streaming `object` result format. It's present only
      // when Druid acknowledges it via this response header.
      let columns: Record<string, { sqlType: string }> | null = null;

      if (response.headers['x-druid-sql-header-included']) {
        for (;;) {
          const line = await lineIterator.next();
          if (line.done) {
            break;
          }
          if (line.value !== '') {
            columns = JSON.parse(line.value);
            break;
          }
        }
      }

      // `objectLines` is newline-delimited JSON and ends with a trailing
      // empty line, so we skip empty lines while parsing the data rows.
      const rowsIterator = (async function* rowsIterator() {
        for (;;) {
          const line = await lineIterator.next();
          if (line.done) {
            break;
          }
          if (line.value !== '') {
            yield JSON.parse(line.value);
          }
        }
      }());

      return {
        rowStream: Readable.from(rowsIterator, { highWaterMark }),
        columns,
        release,
      };
    } catch (e) {
      await release();

      throw e;
    }
  }
}
