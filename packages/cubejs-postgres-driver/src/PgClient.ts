import { Client, Query, QueryResult, ClientConfig, QueryResultRow } from 'pg';

export class PgClient extends Client {
  private readonly clientConfig: ClientConfig;

  public constructor(config: ClientConfig) {
    super(config);
    this.clientConfig = config;
  }

  public isEnding(): boolean {
    return (this as any)._ending;
  }

  public isEnded(): boolean {
    return (this as any)._ended;
  }

  public isQueryable(): boolean {
    return (this as any)._queryable;
  }

  /**
   * Sends PostgreSQL's out-of-band CancelRequest for this client's exact active query.
   * node-postgres requires a separate, otherwise unconnected Client as the control
   * connection used to deliver the request.
   */
  public async cancelQuery(query: Query): Promise<void> {
    // eslint-disable-next-line no-underscore-dangle
    if ((this as any)._activeQuery !== query) {
      return;
    }

    const controlClient = new Client(this.clientConfig);
    // node-postgres intentionally exposes cancel() without exposing the lifecycle
    // of its short-lived protocol connection in its public types.
    const controlConnection = (controlClient as any).connection;

    await new Promise<void>((resolve, reject) => {
      const onError = (error: Error) => {
        // eslint-disable-next-line no-use-before-define
        controlConnection.removeListener('end', onEnd);
        reject(error);
      };
      const onEnd = () => {
        controlConnection.removeListener('error', onError);
        resolve();
      };

      controlConnection.once('error', onError);
      controlConnection.once('end', onEnd);
      (controlClient as any).cancel(this, query);
    });
  }
}

export type PgClientConfig = ClientConfig;
export type PgQueryResult<T extends QueryResultRow = any> = QueryResult<T>;
