import {
  TimeDimensionGranularity,
  TQueryOrderObject,
  TQueryOrderArray,
  moveItemInArray,
  Filter,
  UnaryFilter,
  BinaryFilter,
} from '@cubejs-client/core';
import { BehaviorSubject } from 'rxjs';
import equal from 'fast-deep-equal';

import { Query } from './query';

export type TOrder = 'asc' | 'desc' | 'none';

export type TOrderMember = {
  id: string;
  order: TOrder;
  title: string;
};

export class BaseMember {
  constructor(
    private query: Query,
    private field: 'measures' | 'dimensions' | 'segments'
  ) {}

  private get members() {
    return this.query.asCubeQuery()[this.field] || [];
  }

  add(name: string) {
    this.query.setPartialQuery({
      [this.field]: [...this.members, name],
    });
  }

  replace(name: string, replaceWithName: string) {
    this.query.setPartialQuery({
      [this.field]: this.members.map((currentName) =>
        currentName === name ? replaceWithName : currentName
      ),
    });
  }

  remove(by: string | number) {
    this.query.setPartialQuery({
      [this.field]: this.query
        .asCubeQuery()
        [this.field].filter((currentName, index) => {
          if (typeof by === 'string') {
            return currentName !== by;
          }

          return index !== by;
        }),
    });
  }

  set(members: string[]) {
    this.query.setPartialQuery({
      [this.field]: members,
    });
  }

  asArray() {
    return (this.query.asCubeQuery()[this.field] || []).map((name) =>
      this.query.meta.resolveMember(name, this.field)
    );
  }
}

export class TimeDimensionMember {
  constructor(private query: Query) {}

  private get members() {
    return this.query.asCubeQuery().timeDimensions || [];
  }

  get granularity() {
    return this.members[0]?.granularity;
  }

  updateTimeDimension(by: string | number, updateWith: any) {
    const timeDimensions = this.members.map((td, index) => {
      if (td.dimension === by || index === by) {
        return {
          ...td,
          ...updateWith,
        };
      }
      return td;
    });

    this.query.setPartialQuery({
      timeDimensions,
    });
  }

  add(name: string) {
    this.query.setPartialQuery({
      timeDimensions: [
        {
          dimension: name,
        },
      ],
    });
  }

  remove(name: string) {
    this.query.setPartialQuery({
      timeDimensions: this.members.filter(
        ({ dimension }) => dimension !== name
      ),
    });
  }

  set(timeDimensions: any[]) {
    this.query.setPartialQuery({
      timeDimensions,
    });
  }

  setDateRange(by: string | number, dateRange: string | string[]) {
    this.updateTimeDimension(by, { dateRange });
  }

  setGranularity(by: string | number, granularity: TimeDimensionGranularity) {
    this.updateTimeDimension(by, { granularity });
  }

  asArray(): any[] {
    return (this.query.asCubeQuery().timeDimensions || []).map((td) => {
      return {
        ...this.query.meta.resolveMember(td.dimension, 'dimensions'),
        ...td,
      };
    });
  }
}

export class Order {
  orderMembers = new BehaviorSubject<TOrderMember[]>([]);

  constructor(private query: Query) {
    this.query.subject.subscribe(this.handleQueryChange.bind(this));
    this.orderMembers.subscribe(this.handleOrderMembersChange.bind(this));
  }

  private handleOrderMembersChange(orderMembers: TOrderMember[]) {
    const order = orderMembers
      .filter(({ order }) => order !== 'none')
      .reduce(
        (memo, { id, order }) => ({ ...memo, [id]: order }),
        {}
      ) as TQueryOrderObject;

    if (!equal(order, this.asObject())) {
      this.query.setPartialQuery({ order });
    }
  }

  private handleQueryChange() {
    this.orderMembers.next(
      [
        ...this.query.measures.asArray(),
        ...this.query.dimensions.asArray(),
        ...this.query.timeDimensions.asArray(),
      ].map<TOrderMember>(({ name, title }) => {
        return {
          id: name,
          order: this.of(name),
          title,
        };
      })
    );
  }

  setMemberOrder(id: string, order: TOrder) {
    this.orderMembers.next(
      this.orderMembers.getValue().map((orderMember) => {
        if (orderMember.id === id) {
          return {
            ...orderMember,
            order,
          };
        }
        return orderMember;
      })
    );
  }

  reorder(sourceIndex: number, destinationIndex: number) {
    this.orderMembers.next(
      moveItemInArray(
        this.orderMembers.getValue(),
        sourceIndex,
        destinationIndex
      )
    );
  }

  of(member: string) {
    return (this.query.asCubeQuery().order || {})[member] || 'none';
  }

  set(order: TQueryOrderObject | TQueryOrderArray) {
    this.query.setPartialQuery({ order });
  }

  asArray(): TQueryOrderArray {
    if (Array.isArray(this.query.asCubeQuery().order)) {
      return this.query.asCubeQuery().order as TQueryOrderArray;
    }

    return Object.entries(this.query.asCubeQuery().order || {});
  }

  asObject(): TQueryOrderObject {
    return this.asArray().reduce(
      (memo, [key, value]) => ({ ...memo, [key]: value }),
      {}
    );
  }
}

export class FilterMember {
  constructor(private query: Query) {}

  private get filters() {
    // TODO: update this type assertion once the QueryBuilder supports logical and/or
    return (this.query.asCubeQuery().filters || []) as (UnaryFilter | BinaryFilter)[];
  }

  update(by: string | number, updateWith: Partial<Filter>) {
    const filters = this.filters.map((filter, index) => {
      if (index === by || filter.member === by || filter.dimension === by) {
        return {
          ...filter,
          ...updateWith,
        };
      }
      return filter;
    });

    this.query.setPartialQuery({
      filters: filters as Filter[],
    });
  }

  add(filter: Filter) {
    this.query.setPartialQuery({
      filters: [...this.filters, filter],
    });
  }

  remove(by: string | number) {
    this.query.setPartialQuery({
      filters: this.filters.filter((filter, index) => {
        if (filter.member === by || filter.dimension === by || index === by) {
          return false;
        }

        return true;
      }),
    });
  }

  set(filters: Filter[]) {
    this.query.setPartialQuery({
      filters,
    });
  }

  replace(name: string, replaceWithName: string) {
    this.query.setPartialQuery({
      filters: this.filters.map((filter) => {
        const field = filter.member ? 'member' : 'dimension';
        return filter.member || filter.dimension === name
          ? {
              ...filter,
              [field]: replaceWithName,
            }
          : filter;
      }),
    });
  }

  asArray(): any[] {
    return this.filters.map((filter) => {
      return {
        ...this.query.meta.resolveMember(filter.member || filter.dimension, [
          'dimensions',
          'measures',
        ]),
        operators: this.query.meta.filterOperatorsForMember(
          filter.member || filter.dimension,
          ['dimensions', 'measures']
        ),
        ...filter,
      };
    });
  }
}
