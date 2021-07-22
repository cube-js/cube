import { Injectable, Inject } from '@angular/core';
import { Observable, from, BehaviorSubject } from 'rxjs';
import cubejs, {
  CubejsApi,
  CubeJSApiOptions,
  DryRunResponse,
  LoadMethodOptions,
  Meta,
  Query,
  ResultSet,
  SqlQuery,
} from '@cubejs-client/core';

export type CubejsConfig = {
  token: string;
  options?: CubeJSApiOptions;
};

@Injectable()
export class CubejsClient {
  public ready$: BehaviorSubject<boolean> = new BehaviorSubject(false);

  private cubeJsApi: CubejsApi;

  constructor(@Inject('config') private config: any | Observable<any>) {
    if (this.config instanceof Observable) {
      this.config.subscribe(() => {
        this.ready$.next(true);
      });
    } else {
      this.ready$.next(true);
    }
  }

  private apiInstace(): CubejsApi {
    if (!this.cubeJsApi) {
      if (this.config instanceof Observable) {
        this.config.subscribe((config) => {
          this.cubeJsApi = cubejs(config.token, config.options);

          if (!this.cubeJsApi) {
            throw new Error(
              'Cannot create CubejsApi instance. Please check that the config is passed correctly and contains all required options.'
            );
          }
        });
      } else {
        this.cubeJsApi = cubejs(this.config.token, this.config.options);
      }
    }

    return this.cubeJsApi;
  }

  public load(
    query: Query | Query[],
    options?: LoadMethodOptions
  ): Observable<ResultSet> {
    return from(<Promise<ResultSet>>this.apiInstace().load(query, options));
  }

  public sql(
    query: Query | Query[],
    options?: LoadMethodOptions
  ): Observable<SqlQuery> {
    return from(this.apiInstace().sql(query, options));
  }

  public dryRun(
    query: Query | Query[],
    options?: LoadMethodOptions
  ): Observable<DryRunResponse> {
    return from(this.apiInstace().dryRun(query, options));
  }

  public meta(options?: LoadMethodOptions): Observable<Meta> {
    return from(this.apiInstace().meta(options));
  }

  public watch(query, params = {}): Observable<ResultSet> {
    return new Observable((observer) =>
      query.subscribe({
        next: async (query) => {
          const resultSet = await this.apiInstace().load(query, params);
          observer.next(resultSet);
        },
      })
    );
  }
}
