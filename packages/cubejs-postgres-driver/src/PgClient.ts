import { Client, QueryResult, ClientConfig, QueryResultRow } from 'pg';

import { attachListeners } from './DataRowParser';

export class PgClient extends Client {
  public constructor(config?: string | ClientConfig) {
    super(config);

    // Decode result rows with DataRowParser; pg attaches the parser to the socket on connect()
    (this as any).connection.attachListeners = attachListeners;
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
}

export type PgClientConfig = ClientConfig;
export type PgQueryResult<T extends QueryResultRow = any> = QueryResult<T>;
