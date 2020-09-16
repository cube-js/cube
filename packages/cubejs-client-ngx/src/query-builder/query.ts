import { Meta, Query as TCubeQuery } from '@cubejs-client/core';

import { StateSubject } from './common';
import { BaseMember, Order, TimeDimensionMember } from './query-members';

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
  order: Order;

  constructor(
    initialQuery: TCubeQuery = {},
    public meta: Meta,
    private _onBeforeChange: OnChangeCallback = (newQuery) => newQuery
  ) {
    super(initialQuery);
    this.init(initialQuery);
  }

  private init(query: TCubeQuery) {
    this.measures = new BaseMember(this, MemberType.Measures);
    this.dimensions = new BaseMember(this, MemberType.Dimensions);
    this.segments = new BaseMember(this, MemberType.Segments);
    this.timeDimensions = new TimeDimensionMember(this);
    this.order = new Order(this);

    this.setQuery(query);
  }

  asCubeQuery(): TCubeQuery {
    return this.subject.value || {};
  }

  setQuery(query: TCubeQuery) {
    this.subject.next(this._onBeforeChange(query, this.subject.value, this));
  }

  setPartialQuery(partialQuery: Partial<TCubeQuery>) {
    this.subject.next(
      this._onBeforeChange(
        {
          ...this.subject.value,
          ...partialQuery,
        },
        this.subject.value,
        this
      )
    );
  }

  setLimit(limit: number) {
    this.setPartialQuery({ limit });
  }
}
