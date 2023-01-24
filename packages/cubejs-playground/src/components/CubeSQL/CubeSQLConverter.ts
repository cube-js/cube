import {
  BinaryFilter,
  Query,
  TQueryOrderArray,
  TQueryOrderObject,
  UnaryFilter,
} from '@cubejs-client/core';

import { getMembersList } from './utils';

enum MemberKind {
  Measure,
  Dimension,
  Segment,
  TimeDimension,
}

type ConverterMember = {
  kind: MemberKind;
  name: string;
  position?: number;
  granularity?: string;
};

export class CubeSQLConverter {
  protected members: ConverterMember[] = [];

  protected tables: string[] = [];

  public constructor(protected readonly query: Query) {
    this.decompose();
  }

  public convert() {
    return this.makeQuery();
  }

  protected decompose() {
    this.query.measures?.forEach((name) =>
      this.members.push({
        name,
        kind: MemberKind.Measure,
      })
    );

    this.query.dimensions?.forEach((name) =>
      this.members.push({
        name,
        kind: MemberKind.Dimension,
      })
    );

    this.query.segments?.forEach((name) =>
      this.members.push({
        name,
        kind: MemberKind.Segment,
      })
    );

    this.query.timeDimensions?.forEach((td) =>
      this.members.push({
        name: td.dimension,
        kind: MemberKind.TimeDimension,
        granularity: td.granularity,
      })
    );

    const pathsList = getMembersList(this.query);
    this.tables = Array.from(
      new Set(pathsList.map((path) => path.split('.')[0]))
    );
  }

  protected makeQuery() {
    const query: string[] = [];

    query.push('SELECT');
    const members = this.members.map((member, index) => {
      member.position = index;

      if (member.kind === MemberKind.Measure) {
        return this.makeMeasure(member.name);
      } else if (member.kind === MemberKind.Dimension) {
        return this.makeDimension(member.name);
      } else if (member.kind === MemberKind.TimeDimension) {
        return this.makeTimeDimension(member.name, member.granularity);
      }
    });
    query.push(members.join(', '));

    query.push(`FROM ${this.tables[0]}`);

    query.push(this.makeJoins());

    if (this.query.filters?.length) {
      const filters = this.query.filters.map((filter) =>
        this.makeFilter(filter as any)
      );

      if (filters) {
        query.push(`WHERE ${filters.join(' AND ')}`);
      }
    }

    const groupBy = this.members
      .filter((member) => member.kind !== MemberKind.Measure)
      .map((member) => {
        return member.name;
      });

    if (groupBy) {
      query.push(`GROUP BY ${groupBy.join(', ')}`);
    }

    if (this.query.order) {
      query.push(this.makeOrderBy(this.query.order));
    }

    if (this.query.limit) {
      query.push(`LIMIT ${this.query.limit}`);
    }

    return query.join(' ');
  }

  protected makeJoins() {
    if (this.tables.length > 1) {
      // Naive approach
      const [main, ...otherTables] = this.tables;

      return otherTables
        .map((tableName) => {
          return `LEFT JOIN ${tableName} ON ${main}.__cubeJoinField = ${tableName}.__cubeJoinField`;
        })
        .join(' ');
    }

    return '';
  }

  protected makeMeasure(name: string) {
    return `MEASURE(${name})`;
  }

  // todo: support boolean filters
  protected makeFilter(filter: BinaryFilter | UnaryFilter) {
    if (filter.operator === 'inDateRange') {
      return [
        filter.member,
        '>',
        this.escapeValue(filter.values[0]),
        'AND',
        filter.member,
        '<',
        this.escapeValue(filter.values[1]),
      ].join(' ');
    }

    if (['equals', 'notEquals'].includes(filter.operator) && filter.values) {
      const hasSingleValue = filter.values.length <= 1;
      const operator = {
        equals: hasSingleValue ? '=' : 'IN',
        notEquals: hasSingleValue ? '!=' : 'NOT IN',
      };

      const nullValue =
        filter.operator === 'notEquals' ? `OR ${filter.member} IS NULL` : '';

      const value = hasSingleValue
        ? this.escapeValue(filter.values[0])
        : `(${filter.values.map(this.escapeValue).join(', ')})`;

      const string = [
        filter.member,
        operator[filter.operator],
        value,
        nullValue,
      ]
        .join(' ')
        .trim();

      return `(${string})`;
    }

    return '';
  }

  protected makeDimension(name: string) {
    return name;
  }

  protected makeTimeDimension(name: string, granularity?: string) {
    if (!granularity) {
      return;
    }

    return `DATE_TRUNC('${granularity}', ${name})`;
  }

  protected escapeValue(value: string) {
    return `'${value}'`;
  }

  protected makeOrderBy(queryOrder: TQueryOrderObject | TQueryOrderArray) {
    const orderArray = Array.isArray(queryOrder)
      ? queryOrder
      : Object.entries(queryOrder);

    return `ORDER BY ${orderArray
      .map(([member, order]) => `${member} ${order.toUpperCase()}`)
      .join(', ')
      .trim()}`;
  }
}
