import {
  TimeDimensionGranularity,
  QueryOrder as TQueryOrder,
  TQueryOrderObject,
  TQueryOrderArray,
} from '@cubejs-client/core';

import { Query } from './query';

export class BaseMember {
  constructor(private query: Query, private field: any) {}

  private get members() {
    return this.query.asCubeQuery()[this.field] || [];
  }

  add(name: string) {
    this.query.setPartialQuery({
      [this.field]: [...this.members, name],
    });
  }

  remove(name: string) {
    this.query.setPartialQuery({
      [this.field]: this.query.asCubeQuery()[this.field].filter((currentName) => currentName !== name),
    });
  }

  set(members: string[]) {
    this.query.setPartialQuery({
      [this.field]: members,
    });
  }

  asArray() {
    return this.query.asCubeQuery()[this.field] || [];
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
    let nextTimeDimensions = [];

    if (typeof by === 'number') {
      nextTimeDimensions = this.members.map((td, index) => {
        if (index === by) {
          return {
            ...td,
            ...updateWith,
          };
        }
        return td;
      });
    } else if (by === 'string') {
      nextTimeDimensions = this.members.map((td) => {
        if (td.dimension === by) {
          return {
            ...td,
            ...updateWith,
          };
        }
        return td;
      });
    }

    this.query.setPartialQuery({
      timeDimensions: nextTimeDimensions,
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
    throw new Error('Not implemented');
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

  asArray() {
    return this.query.asCubeQuery().timeDimensions || [];
  }
}

export class Order {
  constructor(private query: Query) {}

  reorder(sourceIndex, destinationIndex) {
    // todo
    throw new Error('Not implemented');
  }

  set(order: TQueryOrderObject | TQueryOrderArray) {
    this.query.setPartialQuery({ order });
  }

  asArray() {
    if (Array.isArray(this.query.asCubeQuery().order)) {
      return this.query.asCubeQuery().order;
    }

    throw new Error('Not implemented');
  }
}
