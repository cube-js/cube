import { isQueryPresent, Meta, Query as TCubeQuery } from '@cubejs-client/core';

import { StateSubject } from './common';
import { BaseMember, FilterMember, Order, TimeDimensionMember } from './query-members';

export enum MemberType {
  Measures = 'measures',
  Dimensions = 'dimensions',
  Segments = 'segments',
  TimeDimensions = 'timeDimensions',
  Filters = 'filters',
  Order = 'order',
}

export type OnChangeCallback = (
  newQuery: TCubeQuery,
  oldQuery: TCubeQuery,
  query: Query
) => TCubeQuery;

export class Query extends StateSubject<TCubeQuery> {
  measures: BaseMember;
  dimensions: BaseMember;
  segments: BaseMember;
  timeDimensions: TimeDimensionMember;
  filters: FilterMember;
  order: Order;

  constructor(
    public meta: Meta,
    private _onBeforeChange: OnChangeCallback = (newQuery) => newQuery
  ) {
    super({});
    this.init();
  }

  private init() {
    this.measures = new BaseMember(this, MemberType.Measures);
    this.dimensions = new BaseMember(this, MemberType.Dimensions);
    this.segments = new BaseMember(this, MemberType.Segments);
    this.timeDimensions = new TimeDimensionMember(this);
    this.filters = new FilterMember(this);
    this.order = new Order(this);
  }

  asCubeQuery(): TCubeQuery {
    return this.subject.getValue() || {};
  }

  setQuery(query: TCubeQuery) {
    this.subject.next(this._onBeforeChange(query, this.subject.getValue(), this));
  }

  setPartialQuery(partialQuery: Partial<TCubeQuery>) {
    this.subject.next(
      this._onBeforeChange(
        {
          ...this.subject.getValue(),
          ...partialQuery,
        },
        this.subject.getValue(),
        this
      )
    );
  }

  setLimit(limit: number) {
    this.setPartialQuery({ limit });
  }
  
  isPresent(): boolean {
    return isQueryPresent(this.asCubeQuery());
  }
}
