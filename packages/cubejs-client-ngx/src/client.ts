import { Injectable, Inject } from '@angular/core';
import { Observable, from, BehaviorSubject } from 'rxjs';
import cube, {
  CubeApi,
  CubeApiOptions,
  DryRunResponse,
  LoadMethodOptions,
  Meta,
  Query,
  ResultSet,
  SqlQuery,
} from '@cubejs-client/core';

export type CubeConfig = {
  token: string;
  options?: CubeApiOptions;
};

@Injectable()
export class CubeClient {
  public ready$: BehaviorSubject<boolean> = new BehaviorSubject(false);

  private cubeApi: CubeApi | undefined;

  private latestConfig: CubeConfig | undefined;

  public constructor(@Inject('config') private config: any | Observable<any>) {
    if (this.config instanceof Observable) {
      // A single subscription keeps the last emitted config, so a cold source
      // such as a bare Subject is not re-subscribed (and its already emitted
      // value lost) on the first request.
      this.config.subscribe((config) => {
        this.latestConfig = config;
        this.cubeApi = undefined;
        this.ready$.next(true);
      });
    } else {
      this.latestConfig = this.config;
      this.ready$.next(true);
    }
  }

  private apiInstance(): CubeApi {
    if (!this.cubeApi) {
      if (!this.latestConfig) {
        throw new Error(
          'Cannot create CubeApi instance. The config observable has not emitted yet, use ready$ to wait for it.'
        );
      }

      this.cubeApi = cube(this.latestConfig.token, this.latestConfig.options);

      if (!this.cubeApi) {
        throw new Error(
          'Cannot create CubeApi instance. Please check that the config is passed correctly and contains all required options.'
        );
      }
    }

    return this.cubeApi;
  }

  public load(
    query: Query | Query[],
    options?: LoadMethodOptions
  ): Observable<ResultSet<any>> {
    return from(this.apiInstance().load(query, options) as Promise<ResultSet<any>>);
  }

  public sql(
    query: Query | Query[],
    options?: LoadMethodOptions
  ): Observable<SqlQuery> {
    return from(this.apiInstance().sql(query, options));
  }

  public dryRun(
    query: Query | Query[],
    options?: LoadMethodOptions
  ): Observable<DryRunResponse> {
    return from(this.apiInstance().dryRun(query, options));
  }

  public meta(options?: LoadMethodOptions): Observable<Meta> {
    return from(this.apiInstance().meta(options));
  }

  public watch(query, params = {}): Observable<ResultSet<any>> {
    return new Observable((observer) => query.subscribe({
      next: async (currentQuery) => {
        try {
          const resultSet = await this.apiInstance().load(currentQuery, params);
          observer.next(resultSet);
        } catch (err) {
          observer.error(err);
        }
      },
    }));
  }
}
