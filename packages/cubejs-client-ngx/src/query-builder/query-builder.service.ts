import { Injectable } from '@angular/core';
import { Meta, ResultSet } from '@cubejs-client/core';
import { BehaviorSubject, Subject } from 'rxjs';

import { CubejsClient } from '../client';
import { Query } from './query';
import { BuilderMeta } from './builder-meta';
import { PivotConfig } from './pivot-config';
import { StateSubject } from './common';

// todo: move to core
import { defaultHeuristics } from './tmp';

type TChartType = 'line' | 'bar' | 'number';

@Injectable()
export class QueryBuilderService {
  private cubejs: CubejsClient;
  private _meta: Meta;
  // private _query: Subject<Query> = new Subject();

  readonly builderMeta: Subject<BuilderMeta> = new Subject();
  readonly state: BehaviorSubject<any> = new BehaviorSubject({});

  pivotConfig: PivotConfig;
  // query: Observable<Query> = this._query.asObservable();
  query: BehaviorSubject<Query> = new BehaviorSubject(null);
  chartType: TChartType = 'line';

  private async init() {
    this.pivotConfig = new PivotConfig(null);
    this._meta = (await this.cubejs.meta().toPromise()) as any;

    this.builderMeta.next(new BuilderMeta(this._meta));
    this.query.next(
      new Query({}, this._meta, (newQuery, oldQuery, currentQuery) => {
        const { chartType, shouldApplyHeuristicOrder, query: heuristicQuery } = defaultHeuristics(newQuery, oldQuery, {
          meta: this._meta,
        });

        console.log('onBeforeChange', {
          chartType,
          shouldApplyHeuristicOrder,
        });

        const query = heuristicQuery || newQuery;
        // todo: isQueryPresent
        if (query && Object.keys(query)) {
          this.cubejs
            .dryRun(query)
            .toPromise()
            .then(({ pivotQuery, queryOrder }) => {
              console.log('pivotConfig', ResultSet.getNormalizedPivotConfig(pivotQuery, this.pivotConfig.get()));
              this.pivotConfig.set(ResultSet.getNormalizedPivotConfig(pivotQuery, this.pivotConfig.get()));

              if (shouldApplyHeuristicOrder) {
                currentQuery.order.set(queryOrder.reduce((a, b) => ({ ...a, ...b }), {}));
              }
            });
        }

        if (chartType) {
          this.setChartType(chartType);
        }

        return query;
      })
    );

    this.subscribe();
  }

  setCubejsClient(cubejsClient: CubejsClient) {
    this.cubejs = cubejsClient;
    this.init();
  }

  setChartType(chartType: TChartType) {
    this.chartType = chartType;

    this.setPartialState({
      chartType,
    });
  }

  private subscribe() {
    Object.getOwnPropertyNames(this).forEach((key) => {
      if (this[key] instanceof StateSubject) {
        this[key].subject.subscribe((value) =>
          this.setPartialState({
            [key]: value,
          })
        );
      }
    });
    this.query.subscribe((query) => {
      query.subject.subscribe((cubeQuery) => {
        this.setPartialState({
          query: cubeQuery,
        });
      });
    });
  }

  deserializeState(state) {
    const keyToClassName = {
      pivotConfig: PivotConfig,
    };

    this.query.subscribe((query) => {
      if (query) {
        query.setQuery(state.query); 
      }
    });

    Object.entries(state).forEach(([key, value]) => {
      if (this[key] instanceof StateSubject) {
        const ClassName = keyToClassName[key];
        this[key] = new ClassName(value);
      }
    });

    this.subscribe();
  }

  setPartialState(partialState) {
    this.state.next({
      ...this.state.value,
      ...partialState,
    });
  }
}
