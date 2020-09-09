import { Query as TCubeQuery, QueryOrder as TQueryOrder } from '@cubejs-client/core';
import { BehaviorSubject } from 'rxjs';

import { BaseMember, TimeDimensionMember } from './query-members';

export enum MemberType {
  Measures = 'measures',
  Dimensions = 'dimensions',
  Segments = 'segments',
  TimeDimensions = 'timeDimensions',
  Filters = 'filters',
  Order = 'order',
}

export class Query {
  readonly query: BehaviorSubject<TCubeQuery> = new BehaviorSubject({});
  measures: BaseMember;
  dimensions: BaseMember;
  segments: BaseMember;
  timeDimensions: TimeDimensionMember;
  order: Order;

  constructor(
    initialQuery: TCubeQuery = {},
    private meta: any,
    private onBeforeChange: (newQuery: TCubeQuery, oldQuery: TCubeQuery) => TCubeQuery = (newQuery) => newQuery
  ) {
    this.measures = new BaseMember(this, MemberType.Measures);
    this.dimensions = new BaseMember(this, MemberType.Dimensions);
    this.segments = new BaseMember(this, MemberType.Segments);
    this.timeDimensions = new TimeDimensionMember(this);
    this.order = new Order(initialQuery.order);

    this.setQuery(initialQuery);
  }

  asCubeQuery(): TCubeQuery {
    return this.query.value || {};
  }

  setQuery(query: TCubeQuery) {
    this.query.next(
      this.onBeforeChange(
        query,
        this.query.value
      )
    );
  }
  
  setPartialQuery(partialQuery: Partial<TCubeQuery>) {
    this.query.next(
      this.onBeforeChange(
        {
          ...this.query.value,
          ...partialQuery,
        },
        this.query.value
      )
    );
  }

  setLimit(limit: number) {
    this.setPartialQuery({ limit });
  }
}

class Order {
  private _order: Array<[string, TQueryOrder]>;

  constructor(order: Array<[string, TQueryOrder]> | { [key: string]: string }) {
    this._order = Array.isArray(order) ? order : [['Orders.count', 'desc']];
  }

  reorder(sourceIndex, destinationIndex) {
    // todo
  }

  get() {
    return this._order;
  }

  update() {
    console.log('order has updated');
  }
}

// class PivotConfig {
//   private pivotConfig: Object;

//   constructor(private query: TCubeQuery) {}

//   moveItem(sourceIndex, destinationIndex, sourceAxis, destinationAxis) {
//     return this.pivotConfig;
//   }
// }

// ### API
// queryBuilder.query

// query.measures.add('Sales.count');
// query.measures.remove('Sales.count');

// query.order.reorder(1, 2);
// query.set({ measures: ['Sales.count'] , order: [['Sales.count', 'desc']]})

// queryBuilder.query.measures.add()
// queryBuilder.query.query
