import { Client, ClientConfig, QueryResult, ClientConfig, QueryResultRow } from 'pg';

export class PgClient extends Client {
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
