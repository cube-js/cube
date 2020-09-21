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
import { ChartType, TChartType } from './chart-type';
import { StateSubject } from './common';

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
  private _disableHeuristics: boolean = false;
  private _resolveQuery: (query: Query) => void;
  private _resolveBuilderMeta: (query: BuilderMeta) => void;

  readonly builderMeta = new Promise<BuilderMeta>(
    (resolve) => (this._resolveBuilderMeta = resolve)
  );
  readonly query = new Promise<Query>(
    (resolve) => (this._resolveQuery = resolve)
  );
  readonly state = new BehaviorSubject<TQueryBuilderState>({});

  pivotConfig: PivotConfig;
  chartType: ChartType;

  private async init() {
    this.pivotConfig = new PivotConfig(null);
    this.chartType = new ChartType('line');

    this._cubejs.meta().subscribe((meta) => {
      this._meta = meta;

      this._query = new Query(
        {},
        this._meta,
        this.handleQueryChange.bind(this)
      );
      this._resolveQuery(this._query);
      this._resolveBuilderMeta(new BuilderMeta(this._meta));
    });

    this.subscribe();
  }

  private handleQueryChange(newQuery, oldQuery, currentQuery) {
    const {
      chartType,
      shouldApplyHeuristicOrder,
      query: heuristicQuery,
    } = defaultHeuristics(
      newQuery,
      Object.keys(oldQuery).length ? oldQuery : newQuery,
      {
        meta: this._meta,
      }
    );

    const query = this._disableHeuristics
      ? newQuery
      : heuristicQuery || newQuery;

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
    }

    if (!this._disableHeuristics && chartType) {
      this.chartType.set(chartType);
    }

    return query;
  }

  setCubejsClient(cubejsClient: CubejsClient) {
    this._cubejs = cubejsClient;
    this.init();
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
    this.query.then((query) => {
      query.setQuery(state.query);
    });

    Object.entries(state).forEach(([key, value]) => {
      if (this[key] instanceof StateSubject) {
        this[key].set(value);
      }
    });

    this.subscribe();
  }

  setPartialState(partialState) {
    this.state.next({
      ...this.state.getValue(),
      ...partialState,
    });
  }

  disableHeuristics() {
    this._disableHeuristics = false;
  }

  enableHeuristics() {
    this._disableHeuristics = true;
  }
}
