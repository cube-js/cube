import { Injectable, Inject } from '@angular/core';
import { Observable, from } from 'rxjs';

import cubejs from '@cubejs-client/core';

import { ResultSet, MetaResult } from './types';

@Injectable()
export class CubejsClient {
  private cubeJsApi;
  constructor(@Inject('config') private config) {
  }

  private apiInstace() {
    if(!this.cubeJsApi) {
      if (this.config instanceof Observable) {
        this.config.subscribe((config) => {
          this.cubeJsApi = cubejs(
            config.token,
            config.options
          )
        })
      } else {
        this.cubeJsApi = cubejs(
          this.config.token,
          this.config.options
        );
      }
    }

    return this.cubeJsApi;
  }

  public load(...params):Observable<ResultSet> {
    return from(<Promise<ResultSet>>this.apiInstace().load(...params));
  }

  public sql(...params):Observable<any> {
    return from(this.apiInstace().sql(...params));
  }

  public meta(...params):Observable<MetaResult> {
    return from(<Promise<MetaResult>>this.apiInstace().meta(...params));
  }

  public watch(query, params = {}):Observable<ResultSet> {
    return Observable.create(observer =>
      query.subscribe({
        next: async query => {
          const resultSet = await this.apiInstace().load(query, params);
          observer.next(resultSet);
        }
      })
    );
  }
}