import { BaseQuery, ParamAllocator } from '@cubejs-backend/schema-compiler';

const GRANULARITY_TO_INTERVAL: Record<string, string> = {
  day: 'Day',
  hour: 'Hour',
  minute: 'Minute',
  second: 'Second',
  month: 'Month',
  quarter: 'Quarter',
  year: 'Year',
};

export class FireboltQuery extends BaseQuery {
  public paramAllocator!: ParamAllocator;

  public convertTz(field: string) {
    return `toTimeZone(${field}, '${this.timezone}')`;
  }

  public timeGroupedColumn(granularity: string, dimension: string) {
    if (granularity === 'week') {
      return `toDateTime(toMonday(${dimension}, '${this.timezone}'), '${this.timezone}')`;
    } else {
      const interval = GRANULARITY_TO_INTERVAL[granularity];

      return `toDateTime(${
        granularity === 'second' ? 'toDateTime' : `toStartOf${interval}`
      }(${dimension}, '${this.timezone}'), '${this.timezone}')`;
    }
  }

  public escapeColumnName(name: string) {
    return `"${name}"`;
  }

  public preAggregationLoadSql(
    cube: string,
    preAggregation: any,
    tableName: string
  ) {
    const sqlAndParams = this.preAggregationSql(cube, preAggregation);

    if (tableName.match(/\./)) {
      const [_, name] = tableName.split('.');
      tableName = name;
    }

    return [
      `CREATE DIMENSION TABLE ${tableName} ${this.asSyntaxTable} ${sqlAndParams[0]}`,
      sqlAndParams[1],
    ];
  }

  public preAggregationPreviewSql(tableName: string) {
    if (tableName.match(/\./)) {
      const [_, name] = tableName.split('.');
      tableName = name;
    }

    return this.paramAllocator.buildSqlAndParams(
      `SELECT * FROM ${tableName} LIMIT 1000`
    );
  }
}
