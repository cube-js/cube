import R from 'ramda';
import moment from 'moment-timezone';

import { QueryAlias } from '@cubejs-backend/shared';
import { BaseQuery } from './BaseQuery';
import { BaseFilter } from './BaseFilter';
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
  const abbr = this.zoneAbbr();
  return abbrs[abbr] || abbr;
};

class MssqlParamAllocator extends ParamAllocator {
  paramPlaceHolder(paramIndex) {
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
  escapeWildcardChars(param) {
    return typeof param === 'string' ? param.replace(/([_%])/gi, '[$1]') : param;
  }

  likeIgnoreCase(column, not, param, type) {
    const p = (!type || type === 'contains' || type === 'ends') ? '%' : '';
    const s = (!type || type === 'contains' || type === 'starts') ? '%' : '';
    return `LOWER(${column})${not ? ' NOT' : ''} LIKE CONCAT('${p}', LOWER(${this.allocateParam(param)}) , '${s}')`;
  }
}

export class MssqlQuery extends BaseQuery {
  newFilter(filter) {
    return new MssqlFilter(this, filter);
  }

  convertTz(field) {
    return `TODATETIMEOFFSET(${field}, '${moment().tz(this.timezone).format('Z')}')`;
  }

  timeStampCast(value) {
    return `CAST(${value} AS DATETIME2)`; // TODO
  }

  dateTimeCast(value) {
    return `CAST(${value} AS DATETIME2)`;
  }

  timeGroupedColumn(granularity, dimension) {
    return GRANULARITY_TO_INTERVAL[granularity](dimension);
  }

  newParamAllocator() {
    return new MssqlParamAllocator();
  }

  groupByDimensionLimit() {
    if (this.rowLimit) {
      return this.offset ? ` OFFSET ${parseInt(this.offset, 10)} ROWS FETCH NEXT ${parseInt(this.rowLimit, 10)} ROWS ONLY` : '';
    } else {
      return this.offset ? ` OFFSET ${parseInt(this.offset, 10)} ROWS` : '';
    }
  }

  topLimit() {
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
  groupByClause() {
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
  aggregateSubQueryGroupByClause() {
    const dimensionColumns = this.dimensionColumns(this.escapeColumnName(QueryAlias.AGG_SUB_QUERY_KEYS));
    return dimensionColumns.length ? ` GROUP BY ${dimensionColumns.join(', ')}` : '';
  }

  overTimeSeriesSelect(cumulativeMeasures, dateSeriesSql, baseQuery, dateJoinConditionSql, baseQueryAlias) {
    const forGroupBy = this.timeDimensions.map(
      (t) => `${t.dateSeriesAliasName()}.${this.escapeColumnName('date_from')}`
    );
    const forSelect = this.overTimeSeriesForSelect(cumulativeMeasures);
    return (
      `SELECT ${forSelect} FROM ${dateSeriesSql}` +
      ` LEFT JOIN (${baseQuery}) ${this.asSyntaxJoin} ${baseQueryAlias} ON ${dateJoinConditionSql}` +
      ` GROUP BY ${forGroupBy}`
    );
  }

  nowTimestampSql() {
    return 'CURRENT_TIMESTAMP';
  }

  unixTimestampSql() {
    // eslint-disable-next-line quotes
    return `DATEDIFF(SECOND,'1970-01-01', GETUTCDATE())`;
  }

  preAggregationLoadSql(cube, preAggregation, tableName) {
    const sqlAndParams = this.preAggregationSql(cube, preAggregation);
    return [`SELECT * INTO ${tableName} FROM (${sqlAndParams[0]}) AS PreAggregation`, sqlAndParams[1]];
  }

  wrapSegmentForDimensionSelect(sql) {
    return `CAST((CASE WHEN ${sql} THEN 1 ELSE 0 END) AS BIT)`;
  }

  seriesSql(timeDimension) {
    const values = timeDimension.timeSeries().map(([from, to]) => `('${from}', '${to}')`);
    return `SELECT ${this.dateTimeCast('date_from')} date_from, ${this.dateTimeCast(
      'date_to'
    )} date_to FROM (VALUES ${values}) ${this.asSyntaxTable} dates (date_from, date_to)`;
  }

  subtractInterval(date, interval) {
    const amountInterval = interval.split(' ', 2);
    const negativeInterval = (amountInterval[0]) * -1;
    return `DATEADD(${amountInterval[1]}, ${negativeInterval}, ${date})`;
  }

  addInterval(date, interval) {
    const amountInterval = interval.split(' ', 2);
    return `DATEADD(${amountInterval[1]}, ${amountInterval[0]}, ${date})`;
  }
}
