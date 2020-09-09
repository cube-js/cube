import { Injectable } from '@angular/core';
import { Meta } from '@cubejs-client/core';
import { Observable, Subject } from 'rxjs';

import { CubejsClient } from '../client';
import { Query } from './query';
import { BuilderMeta } from './builder-meta';

type TChartType = 'line' | 'bar' | 'number';

function defaultHeuristic(oldQuery = {}, newQuery: any = {}, meta): any {
  if ((newQuery?.timeDimensions || []).length > 0) {
    return {
      ...oldQuery,
      ...newQuery,
      timeDimensions: [
        {
          ...newQuery.timeDimensions[0],
          granularity: 'day',
        },
      ],
    };
  }

  return {
    ...oldQuery,
    ...newQuery,
    shouldApplyHeuristicOrder: true,
  };
}

@Injectable()
export class QueryBuilderService {
  private cubejs: CubejsClient;
  private _meta: Meta;
  private _query: Subject<Query> = new Subject();

  readonly builderMeta: Subject<BuilderMeta> = new Subject();
  query: Observable<Query> = this._query.asObservable();
  chartType: TChartType = 'line';

  constructor() {}

  private async init() {
    this._meta = (await this.cubejs.meta().toPromise()) as any;
    this.builderMeta.next(new BuilderMeta(this._meta));
    this._query.next(
      new Query({}, this._meta, (newQuery, oldQuery) => {
        const { chartType, shouldApplyHeuristicOrder, ...query } = defaultHeuristic(oldQuery, newQuery, this._meta);

        if (shouldApplyHeuristicOrder) {
          // const { order } = this.cubejs.dryRun(query);
          // console.log('order', order)
        }

        // this.setChartType(chartType);
        return newQuery;
      })
    );
  }

  setCubejsClient(cubejsClient: CubejsClient) {
    this.cubejs = cubejsClient;
    this.init();
  }

  setChartType(chartType: TChartType) {
    this.chartType = chartType;
  }
}
