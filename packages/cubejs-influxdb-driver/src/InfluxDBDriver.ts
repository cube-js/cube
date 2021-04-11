import { BaseDriver } from '@cubejs-backend/query-orchestrator';
import { getEnv } from '@cubejs-backend/shared';
import { InfluxDB, QueryApi } from '@influxdata/influxdb-client';
import { QueryAPI, InfluxQLQuery } from '@influxdata/influxdb-client-apis';

import { InfluxDBQuery } from './InfluxDBQuery';

interface InfluxDBConfiguration {
  url?: string,
}

export class InfluxDBDriver extends BaseDriver {
  protected readonly config: InfluxDBConfiguration;

  protected readonly client: InfluxDB;

  public static dialectClass() {
    return InfluxDBQuery;
  }

  public constructor(config?: InfluxDBConfiguration) {
    super();

    let url = config?.url || process.env.CUBEJS_DB_URL;
    if (!url) {
      const host = process.env.CUBEJS_DB_HOST;
      const port = process.env.CUBEJS_DB_PORT;

      if (host && port) {
        const protocol = getEnv('dbSsl') ? 'https' : 'http';
        url = `${protocol}://${host}:${port}`;
      } else {
        throw new Error('Please specify CUBEJS_DB_URL');
      }
    }

    this.client = new InfluxDB({ url, token: 'mysecrettoken' });

    this.config = {
      ...config,
    };
  }

  public readOnly() {
    return true;
  }

  public async testConnection() {
    //
  }

  public async query(query: string, values: unknown[] = []): Promise<Array<unknown>> {
    const api = new QueryAPI(this.client);
    const response = await api.postQuery({
      body: {
        query: 'SHOW DATABASES',
        type: 'influxql',
        bucket: 'mybucket',
      },
      org: 'myorg',
    });

    console.log(response);

    // const a = await this.client.with(<any>{ org: 'myorg', type: 'influxql', bucket: '_monitoring' }).queryRows(query, {
    //   next(row: string[], tableMeta) {
    //     const o = tableMeta.toObject(row)
    //     // console.log(JSON.stringify(o, null, 2))
    //     console.log(
    //       `${o._time} ${o._measurement} in '${o.location}' (${o.example}): ${o._field}=${o._value}`
    //     )
    //   },
    //   error(error: Error) {
    //     console.error(error)
    //     console.log('\nFinished ERROR')
    //   },
    //   complete() {
    //     console.log('\nFinished SUCCESS')
    //   },
    // });

    throw new Error('Unimplemented');
  }

  public informationSchemaQuery() {
    return 'SHOW DATABASES;';
  }

  public async createSchemaIfNotExists(schemaName: string): Promise<unknown[]> {
    throw new Error('Unable to create schema, Druid does not support it');
  }

  public async getTablesQuery(schemaName: string): Promise<any> {
    throw new Error('Unimplemented');

    return <any>[];
  }

  protected normaliseResponse(res: any) {
    return res;
  }
}
