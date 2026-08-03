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

  private cubeApi: CubeApi;

  constructor(@Inject('config') private config: any | Observable<any>) {
    if (this.config instanceof Observable) {
      this.config.subscribe(() => {
        this.ready$.next(true);
      });
    } else {
      this.ready$.next(true);
    }
  }

  private apiInstance(): CubeApi {
    if (!this.cubeApi) {
      if (this.config instanceof Observable) {
        this.config.subscribe((config) => {
          this.cubeApi = cube(config.token, config.options);

          if (!this.cubeApi) {
            throw new Error(
              'Cannot create CubeApi instance. Please check that the config is passed correctly and contains all required options.'
            );
          }
        });
      } else {
        this.cubeApi = cube(this.config.token, this.config.options);
      }
    }

    return this.cubeApi;
  }

  public load(
    query: Query | Query[],
    options?: LoadMethodOptions
  ): Observable<ResultSet<any>> {
    return from(<Promise<ResultSet<any>>>this.apiInstance().load(query, options));
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
    return new Observable((observer) =>
      query.subscribe({
        next: async (query) => {
          try {
            const resultSet = await this.apiInstance().load(query, params);
            observer.next(resultSet);
          } catch(err) {
            observer.error(err);
          }

        },
      })
    );
  }
}
