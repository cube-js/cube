import {
  BinaryFilter,
  BinaryOperator,
  Filter,
  Query,
  UnaryFilter,
  UnaryOperator,
  MetaResponse,
  LogicalAndFilter,
  LogicalOrFilter,
} from '@cubejs-client/core';

export function getMembersList(query: NormalizedQuery): string[] {
  const list: string[] = [
    ...(query.dimensions || []),
    ...(query.measures || []),
    ...(query.segments || []),
  ];

  query.timeDimensions?.forEach((td) => {
    list.push(td.dimension);
  });

  if (query.order && (query.order || []).length) {
    query.order.forEach(({ id }) => list.push(id));
  }

  return Array.from(new Set(list));
}

function isBinaryOperator(operator: string): operator is BinaryOperator {
  return !['set', 'notSet'].includes(operator);
}

function isBinaryFilter(filter: any): filter is BinaryFilter {
  return !['set', 'notSet'].includes(filter.operator);
}

function isLogicalAndFilter(filter: any): filter is LogicalAndFilter {
  return filter.and != null;
}

function isLogicalOrFilter(filter: any): filter is LogicalOrFilter {
  return filter.or != null;
}

enum MemberKind {
  Measure,
  Dimension,
  Segment,
  TimeDimension,
}

const operators: Partial<Record<BinaryOperator, string>> = {
  gt: '>',
  gte: '>=',
  lt: '<',
  lte: '<=',
  afterDate: '>',
  beforeDate: '<',
};

const operatorAliases: Partial<Record<BinaryOperator, BinaryOperator>> = {
  afterDate: 'gt',
  afterOrOnDate: 'gte',
  beforeDate: 'lt',
  beforeOrOnDate: 'lte',
};

type ConverterMember = {
  kind: MemberKind;
  name: string;
  position?: number;
  granularity?: string;
};

export type NormalizedQuery = Omit<Query, 'order'> & {
  order?: Array<{ id: string; desc: boolean }>;
};

export class CubeSQLConverter {
  protected members: ConverterMember[] = [];

  protected filters: Array<Filter> = [];

  protected tables: string[] = [];

  protected pathToKind: Record<string, MemberKind> = {};

  public constructor(
    protected readonly query: NormalizedQuery,
    protected readonly meta: MetaResponse
  ) {
    for (const cube of meta.cubes) {
      for (const dimension of cube.dimensions) {
        this.pathToKind[dimension.name] =
          dimension.type !== 'time' ? MemberKind.Dimension : MemberKind.TimeDimension;
      }

      for (const measure of cube.measures) {
        this.pathToKind[measure.name] = MemberKind.Measure;
      }
    }

    this.decompose();
  }

  private getPathKind(path: string | undefined) {
    if (!path || this.pathToKind[path] === undefined) {
      throw new Error(`Member '${path}' not found`);
    }

    return this.pathToKind[path];
  }

  protected decompose() {
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

    this.query.timeDimensions?.forEach((td) => {
      if (td.dateRange) {
        if (!Array.isArray(td.dateRange)) {
          throw new Error('Query must be normalized: `dateRange` must be an array');
        }

        this.filters.push({
          member: td.dimension,
          operator: 'inDateRange',
          values: td.dateRange,
        });
      }

      return this.members.push({
        name: td.dimension,
        kind: MemberKind.TimeDimension,
        granularity: td.granularity,
      });
    });

    this.query.measures?.forEach((name) =>
      this.members.push({
        name,
        kind: MemberKind.Measure,
      })
    );

    this.query.filters?.forEach((filter) => {
      this.filters.push(filter);
    });

    const pathsList = getMembersList(this.query);
    this.tables = Array.from(new Set(pathsList.map((path) => path.split('.')[0])));
  }

  public buildQuery() {
    const query: string[] = [];
    const groupBy: string[] = [];

    const members = this.members
      .filter((member) => member.kind !== MemberKind.Segment)
      .map((member, index) => {
        member.position = index;

        if (member.kind !== MemberKind.Measure) {
          groupBy.push((index + 1).toString());
        }

        if (member.kind === MemberKind.Measure) {
          return this.makeMeasure(member.name);
        } else if (member.kind === MemberKind.Dimension) {
          return this.makeDimension(member.name);
        } else if (member.kind === MemberKind.TimeDimension) {
          return this.makeTimeDimension(member.name, member.granularity);
        }

        return '';
      });
    query.push('SELECT');
    query.push(members.join(', '));

    query.push(`FROM ${this.tables[0]}`);

    query.push(this.makeJoins());

    let having: string = '';

    const flattenFilter = (filter: Filter, isMeasureFilter: boolean): string => {
      if (isLogicalAndFilter(filter)) {
        const value = filter.and.map((it) => flattenFilter(it, isMeasureFilter)).join(' AND ');

        return `(${value})`;
      }

      if (isLogicalOrFilter(filter)) {
        const value = filter.or.map((it) => flattenFilter(it, isMeasureFilter)).join(' OR ');

        return `(${value})`;
      }

      return this.makeFilter({
        ...filter,
        member: isMeasureFilter ? this.makeMeasure(filter.member!) : filter.member,
      });
    };

    const allPathsFromFilter = (filter: Filter): string[] => {
      if (isLogicalAndFilter(filter)) {
        return filter.and.flatMap((it) => allPathsFromFilter(it));
      }

      if (isLogicalOrFilter(filter)) {
        return filter.or.flatMap((it) => allPathsFromFilter(it));
      }

      return [filter.member!];
    };

    if (this.filters.length || this.query.segments?.length) {
      const measureFilters = this.filters.filter((filter) => {
        const paths = allPathsFromFilter(filter);

        return paths.every((path) => this.getPathKind(path) === MemberKind.Measure);
      });

      const dimensionFilters = this.filters.filter((filter) => {
        const paths = allPathsFromFilter(filter);

        return paths.every((path) => this.getPathKind(path) === MemberKind.Dimension);
      });

      const filters = flattenFilter(
        {
          and: dimensionFilters,
        },
        false
      );

      if (filters) {
        query.push(`WHERE ${[filters, ...(this.query.segments || [])].join(' AND ')}`);
      }

      if (measureFilters.length) {
        having = flattenFilter({ and: measureFilters }, true);
      }
    }

    if (groupBy.length) {
      query.push(`GROUP BY ${groupBy.join(', ')}`);
    }

    if (having) {
      query.push(`HAVING ${having}`);
    }

    if (this.query.order && (this.query.order || []).length > 0) {
      query.push(this.makeOrderBy(this.query.order));
    }

    if (this.query.limit) {
      query.push(`LIMIT ${this.query.limit}`);
    }

    return `${query.filter(Boolean).join('\n')};`;
  }

  protected makeJoins() {
    if (this.tables.length > 1) {
      const [_, ...otherTables] = this.tables;

      return otherTables
        .map((tableName) => {
          return `CROSS JOIN ${tableName}`;
        })
        .join('  \n');
    }

    return '';
  }

  protected makeMeasure(name: string) {
    return `MEASURE(${name})`;
  }

  protected makeFilter(filter: BinaryFilter | UnaryFilter): string {
    if (
      isBinaryFilter(filter) &&
      isBinaryOperator(filter.operator) &&
      operatorAliases[filter.operator]
    ) {
      const operator = operatorAliases[filter.operator];
      if (operator) {
        return this.makeFilter({
          ...filter,
          operator,
        });
      }
    }

    if (filter.operator === 'set') {
      return `${filter.member} IS NOT NULL`;
    }

    if (filter.operator === 'notSet') {
      return `${filter.member} IS NULL`;
    }

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

    if (filter.operator === 'notInDateRange') {
      return [
        filter.member,
        '<',
        this.escapeValue(filter.values[0]),
        'OR',
        filter.member,
        '>',
        this.escapeValue(filter.values[1]),
      ].join(' ');
    }

    if (['equals', 'notEquals'].includes(filter.operator) && filter.values) {
      const hasSingleValue = filter.values.length <= 1;
      const operator: Partial<Record<BinaryOperator | UnaryOperator, string>> = {
        equals: hasSingleValue ? '=' : 'IN',
        notEquals: hasSingleValue ? '!=' : 'NOT IN',
      };

      const nullValue = filter.operator === 'notEquals' ? `OR ${filter.member} IS NULL` : '';

      const value = hasSingleValue
        ? this.escapeValue(filter.values[0])
        : `(${filter.values.map(this.escapeValue).join(', ')})`;

      const string = [filter.member, operator[filter.operator], value, nullValue].join(' ').trim();

      // return `(${string})`;
      return string;
    }

    const repeatFilter = (template: string, values: string[], negated: boolean) => {
      const preparedFilters = values.map((value) => template.replace('{{value}}', value));

      return preparedFilters.length > 1
        ? `(${preparedFilters.join(negated ? ' AND ' : ' OR ')})`
        : preparedFilters[0];
    };

    if (filter.operator === 'contains') {
      return repeatFilter(`${filter.member} ILIKE '%' || '{{value}}' || '%'`, filter.values, false);
    }

    if (filter.operator === 'notContains') {
      return repeatFilter(
        `${filter.member} NOT ILIKE '%' || '{{value}}' || '%'`,
        filter.values,
        true
      );
    }

    if (filter.operator === 'startsWith') {
      return repeatFilter(`starts_with(${filter.member}, '{{value}}')`, filter.values, false);
    }

    if (filter.operator === 'notStartsWith') {
      return repeatFilter(`${filter.member} NOT LIKE '{{value}}%'`, filter.values, true);
    }

    if (filter.operator === 'endsWith') {
      return repeatFilter(`ends_with(${filter.member}, {{value}})`, filter.values, false);
    }

    if (filter.operator === 'notEndsWith') {
      return repeatFilter(`${filter.member} NOT LIKE '%{{value}}'`, filter.values, true);
    }

    if (filter.values && operators[filter.operator]) {
      const value = this.escapeValue(filter.values[0]);

      return `${filter.member} ${operators[filter.operator]} ${value}`;
    }

    return '';
  }

  protected makeDimension(name: string) {
    return name;
  }

  protected makeTimeDimension(name: string, granularity?: string) {
    if (!granularity) {
      return name;
    }

    return `DATE_TRUNC('${granularity}', ${name})`;
  }

  protected escapeValue(value: string) {
    return `'${value}'`;
  }

  protected makeOrderBy(queryOrder: Array<{ id: string; desc: boolean }>) {
    return `ORDER BY ${queryOrder
      .map(({ id, desc }) => {
        const selectMember = this.members.find((m) => m.name === id);
        if (selectMember?.position == null) {
          return null;
        }

        return `${selectMember.position + 1}${desc ? ' DESC' : ''}`;
      })
      .filter(Boolean)
      .join(', ')
      .trim()}`;
  }
}
