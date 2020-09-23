import Axios from 'axios';

export type DruidClientBaseConfiguration = {
  user?: string,
  password?: string,
  database?: string,
};

export type DruidClientConfiguration = DruidClientBaseConfiguration & {
  url: string,
};

export class DruidClient {
  constructor(
    protected readonly config: DruidClientConfiguration,
  ) {
  }

  protected getClient() {
    return Axios.create({
      baseURL: this.config.url,
      headers: {
        'Content-Type': 'application/json',
      }
    });

    // @todo authorization!?
  }

  public async cancel(queryId: string) {
    return this.getClient().request({
      url: `/druid/v2/${queryId}`,
      method: 'DELETE',
    });
  }

  public async query(query: string, parameters: { type: string, value: unknown }[]) {
    let cancelled = false;
    const cancelObj: any = {};

    const promise: Promise<unknown> & { cancel?: () => void } = (async () => {
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
            resultFormat: 'object',
          },
        });

        if (cancelled) {
          await this.cancel(response.headers['x-druid-sql-query-id']);

          throw new Error('Query cancelled');
        }

        return response.data;
      } catch (e) {
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
