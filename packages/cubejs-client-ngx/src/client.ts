import { Injectable, Inject } from '@angular/core';
import { Observable, from } from 'rxjs';
import cubejs, {
  LoadMethodOptions,
  Meta,
  Query,
  ResultSet,
} from '@cubejs-client/core';

@Injectable()
export class CubejsClient {
  private cubeJsApi;
  constructor(@Inject('config') private config) {}

  private apiInstace() {
    if (!this.cubeJsApi) {
      if (this.config instanceof Observable) {
        this.config.subscribe((config) => {
          this.cubeJsApi = cubejs(config.token, config.options);
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

  public sql(...params): Observable<any> {
    return from(this.apiInstace().sql(...params));
  }

  public dryRun(...params): Observable<any> {
    return from(this.apiInstace().dryRun(...params));
  }

  public meta(...params): Observable<Meta> {
    return from(<Promise<Meta>>this.apiInstace().meta(...params));
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
