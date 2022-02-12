import { BaseFilter, BaseQuery } from '@cubejs-backend/schema-compiler';

const GRANULARITY_TO_INTERVAL = {
  day: (date: string) => `FORMAT_TIMESTAMP(${date}, 'yyyy-MM-dd''T''00:00:00.000')`,
  week: (date: string) => `FORMAT_TIMESTAMP(PARSE_TIMESTAMP(FORMAT_TIMESTAMP(${date}, 'YYYY-ww'), 'YYYY-ww'), 'yyyy-MM-dd''T''00:00:00.000')`,
  hour: (date: string) => `FORMAT_TIMESTAMP(${date}, 'yyyy-MM-dd''T''HH:00:00.000')`,
  minute: (date: string) => `FORMAT_TIMESTAMP(${date}, 'yyyy-MM-dd''T''HH:mm:00.000')`,
  second: (date: string) => `FORMAT_TIMESTAMP(${date}, 'yyyy-MM-dd''T''HH:mm:ss.000')`,
  month: (date: string) => `FORMAT_TIMESTAMP(${date}, 'yyyy-MM-01''T''00:00:00.000')`,
  quarter: (date: string) => `FORMAT_TIMESTAMP(PARSE_TIMESTAMP(FORMAT_TIMESTAMP(${date}, 'YYYY-qq'), 'YYYY-qq'), 'yyyy-MM-dd''T''00:00:00.000')`,
  year: (date: string) => `FORMAT_TIMESTAMP(${date}, 'yyyy-01-01''T''00:00:00.000')`
};

class KsqlFilter extends BaseFilter {
  // eslint-disable-next-line no-use-before-define
  public constructor(query: KsqlQuery, filter: any) {
    super(query, filter);
  }

  public likeIgnoreCase(column: string, not: boolean, param: any) {
    return `${column}${not ? ' NOT' : ''} ILIKE CONCAT('%', ${this.allocateParam(param)}, '%')`;
  }
}

type Granularity = 'year' | 'quarter' | 'month' | 'week' | 'day' | 'hour' | 'minute' | 'second';

export class KsqlQuery extends BaseQuery {
  public newFilter(filter: any) {
    return new KsqlFilter(this, filter);
  }

  public convertTz(field: string) {
    return `CONVERT_TZ(${field}, 'UTC', '${this.timezone}')`;
  }

  public timeStampParam() {
    return 'PARSE_TIMESTAMP(?, \'yyyy-MM-dd\'\'T\'\'HH:mm:ss.SSSX\', \'UTC\')';
  }

  public timeStampCast(value: string) {
    return `CAST(${value} as TIMESTAMP)`;
  }

  public dateTimeCast(value: string) {
    return `CAST(${value} AS TIMESTAMP)`;
  }

  public timeGroupedColumn(granularity: Granularity, dimension: string) {
    return `PARSE_TIMESTAMP(${GRANULARITY_TO_INTERVAL[granularity](dimension)}, 'yyyy-MM-dd''T''HH:mm:ss.SSS', 'UTC')`;
  }

  public escapeColumnName(name: string) {
    return `\`${name}\``;
  }

  public concatStringsSql(strings: string[]) {
    return `CONCAT(${strings.join(', ')})`;
  }

  public unixTimestampSql() {
    return 'UNIX_TIMESTAMP()';
  }

  public preAggregationLoadSql(cube: string, preAggregation: any, tableName: string) {
    const sqlAndParams = this.preAggregationSql(cube, preAggregation);
    return [`CREATE TABLE ${this.escapeColumnName(tableName)} WITH (KEY_FORMAT='JSON') ${this.asSyntaxTable} ${sqlAndParams[0]}`, sqlAndParams[1]];
  }

  public groupByClause() {
    const dimensionsForSelect: any[] = this.dimensionsForSelect();
    const dimensionColumns = dimensionsForSelect.map(s => s.selectColumns() && s.dimensionSql())
      .reduce((a, b) => a.concat(b), [])
      .filter((s: any) => !!s);
    return dimensionColumns.length ? ` GROUP BY ${dimensionColumns.join(', ')}` : '';
  }

  public preAggregationInvalidateKeyQueries(cube: string, preAggregation: any) {
    // always empty as streaming tables are constantly refreshed by Cube Store
    return [];
  }
}
