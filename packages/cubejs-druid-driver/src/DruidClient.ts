import Axios from 'axios';

export type DruidClientConfiguration = {
  host?: string,
  port?: string,
  user?: string,
  password?: string,
  database?: string,
};

export class DruidClient {
  constructor(
    protected readonly config: DruidClientConfiguration,
  ) {
  }

  protected getClient()
  {
    return Axios.create({
      baseURL: `http://${this.config.host}:${this.config.port}/`,
      headers: {
        'Content-Type': 'application/json',
      }
    });
  }

  public async query(query: string, parameters: { type: string, value: unknown }[])
  {
    try {
      const response = await this.getClient().request({
        url: `/druid/v2/sql/`,
        method: 'POST',
        data: {
          query,
          parameters,
          resultFormat: 'object',
        },
      });

      console.log(response.data);
      return response.data;
    } catch (e) {
      console.log(e);

      throw e;
    }
  }

  public async cancel()
  {

  }
}
