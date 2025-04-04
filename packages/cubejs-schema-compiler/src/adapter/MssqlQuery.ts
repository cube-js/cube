import R from 'ramda';
import moment from 'moment-timezone';

import { QueryAlias, parseSqlInterval } from '@cubejs-backend/shared';
import { BaseQuery } from './BaseQuery';
import { BaseFilter } from './BaseFilter';
import { BaseSegment } from './BaseSegment';
import { ParamAllocator } from './ParamAllocator';

const abbrs = {
  EST: 'Eastern Standard Time',
  EDT: 'Eastern Standard Time',
  CST: 'Central Standard Time',
  CDT: 'Central Standard Time',
  MST: 'Mountain Standard Time',
  MDT: 'Mountain Standard Time',
  PST: 'Pacific Standard Time',
  PDT: 'Pacific Standard Time',
};

moment.fn.zoneName = () => {
  // @ts-ignore
  const abbr = this.zoneAbbr();
  return abbrs[abbr] || abbr;
};

class MssqlParamAllocator extends ParamAllocator {
  public paramPlaceHolder(paramIndex) {
    return `@_${paramIndex + 1}`;
  }
}

const GRANULARITY_TO_INTERVAL = {
  day: (date) => `dateadd(day, DATEDIFF(day, 0, ${date}), 0)`,
  week: (date) => `dateadd(week, DATEDIFF(week, 0, ${date}), 0)`,
  hour: (date) => `dateadd(hour, DATEDIFF(hour, 0, ${date}), 0)`,
  minute: (date) => `dateadd(minute, DATEDIFF(minute, 0, ${date}), 0)`,
  second: (date) => `CAST(FORMAT(${date}, 'yyyy-MM-ddTHH:mm:ss.000') AS DATETIME2)`, // until SQL 2016, this causes an int overflow; in SQL 2016 these calls can be changed to DATEDIFF_BIG
  month: (date) => `dateadd(month, DATEDIFF(month, 0, ${date}), 0)`,
  quarter: (date) => `dateadd(quarter, DATEDIFF(quarter, 0, ${date}), 0)`,
  year: (date) => `dateadd(year, DATEDIFF(year, 0, ${date}), 0)`,
};

class MssqlFilter extends BaseFilter {
  // noinspection JSMethodCanBeStatic
  public escapeWildcardChars(param) {
    return typeof param === 'string' ? param.replace(/([_%])/gi, '[$1]') : param;
  }

  public likeIgnoreCase(column, not, param, type) {
    const p = (!type || type === 'contains' || type === 'ends') ? '%' : '';
    const s = (!type || type === 'contains' || type === 'starts') ? '%' : '';
    return `LOWER(${column})${not ? ' NOT' : ''} LIKE CONCAT('${p}', LOWER(${this.allocateParam(param)}) , '${s}')`;
  }
}

class MssqlSegment extends BaseSegment {
  public filterToWhere(): string {
    const where = super.filterToWhere();

    const context = this.query.safeEvaluateSymbolContext();
    if (context.rollupQuery) {
      // Segment itself will be rendered as reference for rollupQuery
      // In MSSQL using just `WHERE (segment_column) AND (other_filter)` is incorrect, because
      // `segment_column` is not of boolean type, but of `BIT` type
      // Correct way to work with them is to use `WHERE segment_column = 1`
      // This relies on `wrapSegmentForDimensionSelect` mapping segment to a `BIT` data type
      return `${where} = 1`;
    }

    return where;
  }
}

export class MssqlQuery extends BaseQuery {
  public newFilter(filter) {
    return new MssqlFilter(this, filter);
  }

  public newSegment(segment): BaseSegment {
    return new MssqlSegment(this, segment);
  }

  public castToString(sql) {
    return `CAST(${sql} as VARCHAR)`;
  }

  public concatStringsSql(strings: string[]) {
    return strings.join(' + ');
  }

  public convertTz(field) {
    return `TODATETIMEOFFSET(${field}, '${moment().tz(this.timezone).format('Z')}')`;
  }

  public timeStampCast(value: string) {
    return `CAST(${value} AS DATETIMEOFFSET)`;
  }

  public dateTimeCast(value: string) {
    return `CAST(${value} AS DATETIME2)`;
  }

  public timeGroupedColumn(granularity: string, dimension: string): string {
    return GRANULARITY_TO_INTERVAL[granularity](dimension);
  }

  /**
   * Returns sql for source expression floored to timestamps aligned with
   * intervals relative to origin timestamp point.
   * The formula operates with seconds diffs so it won't produce human-expected dates aligned with offset date parts.
   */
  public dateBin(interval: string, source: string, origin: string): string {
    const beginOfTime = this.dateTimeCast('DATEFROMPARTS(1970, 1, 1)');
    const timeUnit = this.diffTimeUnitForInterval(interval);

    // Need to explicitly cast one argument of floor to float to trigger correct sign logic
    return `DATEADD(${timeUnit},
        FLOOR(
          CAST(DATEDIFF(${timeUnit}, ${this.dateTimeCast(`'${origin}'`)}, ${source}) AS FLOAT) /
          DATEDIFF(${timeUnit}, ${beginOfTime}, ${this.addInterval(beginOfTime, interval)})
        ) * DATEDIFF(${timeUnit}, ${beginOfTime}, ${this.addInterval(beginOfTime, interval)}),
        ${this.dateTimeCast(`'${origin}'`)}
    )`;
  }

  public newParamAllocator(expressionParams) {
    return new MssqlParamAllocator(expressionParams);
  }

  // TODO replace with limitOffsetClause override
  public groupByDimensionLimit() {
    if (this.rowLimit) {
      return this.offset ? ` OFFSET ${parseInt(this.offset, 10)} ROWS FETCH NEXT ${parseInt(this.rowLimit, 10)} ROWS ONLY` : '';
    } else {
      return this.offset ? ` OFFSET ${parseInt(this.offset, 10)} ROWS` : '';
    }
  }

  public topLimit() {
    if (this.offset) {
      return '';
    }
    return this.rowLimit === null ? '' : ` TOP ${this.rowLimit && parseInt(this.rowLimit, 10) || 10000}`;
  }

  /**
   * Overrides `BaseQuery#groupByClause` method and returns `GROUP BY` clause
   * with the column names instead of column numeric sequences as MSSQL does
   * not support this format.
   * @returns {string}
   * @override
   */
  public groupByClause() {
    if (this.ungrouped) {
      return '';
    }
    const dimensionsForSelect = this.dimensionsForSelect();
    const dimensionColumns = R.flatten(
      dimensionsForSelect.map(s => s.selectColumns() && s.dimensionSql())
    ).filter(s => !!s);
    return dimensionColumns.length ? ` GROUP BY ${dimensionColumns.join(', ')}` : '';
  }

  /**
   * Overrides `BaseQuery#aggregateSubQueryGroupByClause` method and returns
   * `GROUP BY` clause for the "aggregating on top of sub-queries" uses cases.
   * @returns {string}
   * @override
   */
  public aggregateSubQueryGroupByClause() {
    const dimensionColumns = this.dimensionColumns(this.escapeColumnName(QueryAlias.AGG_SUB_QUERY_KEYS));
    return dimensionColumns.length ? ` GROUP BY ${dimensionColumns.join(', ')}` : '';
  }

  public overTimeSeriesSelect(cumulativeMeasures, dateSeriesSql, baseQuery, dateJoinConditionSql, baseQueryAlias) {
    // Group by time dimensions
    const timeDimensionsColumns = this.timeDimensions.map(
      (t) => `${t.dateSeriesAliasName()}.${this.escapeColumnName('date_from')}`
    );

    // Group by regular dimensions
    const dimensionColumns = R.flatten(
      this.dimensions.map(s => s.selectColumns() && s.dimensionSql() && s.aliasName())
    ).filter(s => !!s);

    // Combine time dimensions and regular dimensions for GROUP BY clause
    const allGroupByColumns = timeDimensionsColumns.concat(dimensionColumns);

    const forSelect = this.overTimeSeriesForSelect(cumulativeMeasures);
    return (
      `SELECT ${forSelect} FROM ${dateSeriesSql}` +
      ` LEFT JOIN (${baseQuery}) ${this.asSyntaxJoin} ${baseQueryAlias} ON ${dateJoinConditionSql}` +
      ` GROUP BY ${allGroupByColumns.join(', ')}`
    );
  }

  public nowTimestampSql() {
    return 'CURRENT_TIMESTAMP';
  }

  public unixTimestampSql() {
    // eslint-disable-next-line quotes
    return `DATEDIFF(SECOND,'1970-01-01', GETUTCDATE())`;
  }

  public preAggregationLoadSql(cube, preAggregation, tableName) {
    const sqlAndParams = this.preAggregationSql(cube, preAggregation);
    return [`SELECT * INTO ${tableName} FROM (${sqlAndParams[0]}) AS PreAggregation`, sqlAndParams[1]];
  }

  public wrapSegmentForDimensionSelect(sql) {
    return `CAST((CASE WHEN ${sql} THEN 1 ELSE 0 END) AS BIT)`;
  }

  public seriesSql(timeDimension) {
    const values = timeDimension.timeSeries().map(([from, to]) => `('${from}', '${to}')`);
    return `SELECT ${this.dateTimeCast('date_from')} date_from, ${this.dateTimeCast(
      'date_to'
    )} date_to FROM (VALUES ${values}) ${this.asSyntaxTable} dates (date_from, date_to)`;
  }

  public subtractInterval(date: string, interval: string): string {
    const intervalParsed = parseSqlInterval(interval);
    let res = date;

    for (const [key, value] of Object.entries(intervalParsed)) {
      res = `DATEADD(${key}, ${value * -1}, ${res})`;
    }

    return res;
  }

  public addInterval(date: string, interval: string): string {
    const intervalParsed = parseSqlInterval(interval);
    let res = date;

    for (const [key, value] of Object.entries(intervalParsed)) {
      res = `DATEADD(${key}, ${value}, ${res})`;
    }

    return res;
  }

  public sqlTemplates() {
    const templates = super.sqlTemplates();
    templates.functions.LEAST = 'LEAST({{ args_concat }})';
    templates.functions.GREATEST = 'GREATEST({{ args_concat }})';
    delete templates.expressions.ilike;
    // NOTE: this template contains a comma; two order expressions are being generated
    templates.expressions.sort = '{{ expr }} IS NULL {% if nulls_first %}DESC{% else %}ASC{% endif %}, {{ expr }} {% if asc %}ASC{% else %}DESC{% endif %}';
    templates.types.string = 'VARCHAR';
    templates.types.boolean = 'BIT';
    templates.types.integer = 'INT';
    templates.types.float = 'FLOAT(24)';
    templates.types.double = 'FLOAT(53)';
    templates.types.timestamp = 'DATETIME2';
    delete templates.types.interval;
    templates.types.binary = 'VARBINARY';
    return templates;
  }
}
