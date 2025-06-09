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

  public likeIgnoreCase(column: string, not: boolean, param: any, type: string) {
    const p = (!type || type === 'contains' || type === 'ends') ? '%' : '';
    const s = (!type || type === 'contains' || type === 'starts') ? '%' : '';
    return `${column}${not ? ' NOT' : ''} ILIKE CONCAT('${p}', ${this.allocateParam(param)}, '${s}')`;
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

  public castToString(sql: string) {
    return `CAST(${sql} as varchar(255))`;
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
    if (this.ungrouped) {
      return '';
    }
    const dimensionsForSelect: any[] = this.dimensionsForSelect();
    const dimensionColumns = dimensionsForSelect.map(s => s.selectColumns() && s.dimensionSql())
      .reduce((a, b) => a.concat(b), [])
      .filter((s: any) => !!s);
    return dimensionColumns.length ? ` GROUP BY ${dimensionColumns.join(', ')}` : '';
  }

  public preAggregationStartEndQueries(cube: string, preAggregation: any) {
    if (preAggregation.partitionGranularity) {
      if (!preAggregation.refreshRangeStart) {
        throw new Error('Pre aggregation schema for kSql source with partition granularity must contains buildRangeStart parameter');
      }
      if (!preAggregation.refreshRangeEnd) {
        throw new Error('Pre aggregation schema for kSql source with partition granularity must contains buildRangeEnd parameter');
      }
    }
    const res = this.evaluateSymbolSqlWithContext(() => [

      preAggregation.refreshRangeStart && [this.evaluateSql(cube, preAggregation.refreshRangeStart.sql, {}), [], { external: true }],
      preAggregation.refreshRangeEnd && [this.evaluateSql(cube, preAggregation.refreshRangeEnd.sql, {}), [], { external: true }]
    ], { preAggregationQuery: true });
    return res;
  }

  public preAggregationReadOnly(cube: string, preAggregation: any) {
    const [sql] = this.preAggregationSql(cube, preAggregation);
    return preAggregation.type === 'originalSql' && Boolean(KsqlQuery.extractTableFromSimpleSelectAsteriskQuery(sql)) ||
      preAggregation.type === 'rollup' && !!this.dimensionsForSelect().find(d => d.definition().primaryKey);
  }

  public preAggregationAllowUngroupingWithPrimaryKey(_cube: any, _preAggregation: any) {
    return true;
  }

  public static extractTableFromSimpleSelectAsteriskQuery(sql: string) {
    const match = sql.replace(/\n/g, ' ').match(/^\s*select\s+.*\s+from\s+([a-zA-Z0-9_\-`".*]+)\s*/i);
    return match && match[1];
  }
}
