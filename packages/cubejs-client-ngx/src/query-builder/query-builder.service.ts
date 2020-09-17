import { Injectable } from '@angular/core';
import {
  Meta,
  ResultSet,
  Query as TCubeQuery,
  PivotConfig as TPivotConfig,
  isQueryPresent,
  defaultHeuristics,
} from '@cubejs-client/core';
import { BehaviorSubject, Subject } from 'rxjs';

import { CubejsClient } from '../client';
import { Query } from './query';
import { BuilderMeta } from './builder-meta';
import { PivotConfig } from './pivot-config';
import { StateSubject } from './common';

export type TChartType = 'line' | 'bar' | 'number' | 'table' | 'pie';

export type TQueryBuilderState = {
  query?: TCubeQuery;
  pivotConfig?: TPivotConfig;
  chartType?: TChartType;
};

@Injectable()
export class QueryBuilderService {
  private _cubejs: CubejsClient;
  private _meta: Meta;
  private _query: Query;
  private _resolveQuery: (query: Query) => void;

  readonly builderMeta = new Subject<BuilderMeta>();
  readonly state = new BehaviorSubject<TQueryBuilderState>({});

  pivotConfig: PivotConfig;
  query = new Promise<Query>((resolve) => (this._resolveQuery = resolve));
  chartType: TChartType = 'line';

  private async init() {
    this.pivotConfig = new PivotConfig(null);

    this._cubejs.meta().subscribe((meta) => {
      this._meta = meta;
      this.builderMeta.next(new BuilderMeta(this._meta));

      this._query = new Query(
        {},
        this._meta,
        this.handleQueryChange.bind(this)
      );
      this._resolveQuery(this._query);
    });

    this.subscribe();
  }

  private handleQueryChange(newQuery, oldQuery, currentQuery) {
    const {
      chartType,
      shouldApplyHeuristicOrder,
      query: heuristicQuery,
    } = defaultHeuristics(newQuery, oldQuery, {
      meta: this._meta,
    });

    const query = heuristicQuery || newQuery;

    // console.log('onBeforeChange', {
    //   chartType,
    //   shouldApplyHeuristicOrder,
    //   newQuery,
    //   oldQuery,
    //   heuristicQuery,
    //   'isQueryPresent(query)': isQueryPresent(query),
    // });

    if (isQueryPresent(query)) {
      this._cubejs
        .dryRun(query)
        .toPromise()
        .then(({ pivotQuery, queryOrder }) => {
          this.pivotConfig.set(
            ResultSet.getNormalizedPivotConfig(
              pivotQuery,
              this.pivotConfig.get()
            )
          );

          if (shouldApplyHeuristicOrder) {
            currentQuery.order.set(
              queryOrder.reduce((a, b) => ({ ...a, ...b }), {})
            );
          }
        });
    } else {
      this.pivotConfig.set({ x: [], y: [] });
    }

    if (chartType) {
      this.setChartType(chartType);
    }

    return query;
  }

  setCubejsClient(cubejsClient: CubejsClient) {
    this._cubejs = cubejsClient;
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
    this.query.then((query) => {
      query.subject.subscribe((cubeQuery) => {
        this.setPartialState({
          query: cubeQuery,
        });
      });
    });
  }

  deserialize(state) {
    const keyToClassName = {
      pivotConfig: PivotConfig,
    };

    this.query.then((query) => {
      query.setQuery(state.query);
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
