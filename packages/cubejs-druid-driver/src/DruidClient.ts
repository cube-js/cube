import Axios, { AxiosRequestConfig } from 'axios';

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
}
