import moment from 'moment-timezone';

import { BaseQuery } from './BaseQuery';
import { BaseFilter } from './BaseFilter';

const GRANULARITY_TO_INTERVAL = {
  day: (date) => `TO_VARCHAR(${date}, 'YYYY-MM-DD') || 'T00:00:00.000'`,
  week: (date) => `TO_VARCHAR(${date}, 'YYYY-MM-DD') || 'T00:00:00.000'`,
  hour: (date) => `TO_VARCHAR(${date}, 'yyyy-MM-dd"T"HH') || ':00:00.000'`,
  minute: (date) => `TO_VARCHAR(${date}, 'yyyy-MM-dd"T"HH:MM') || ':00.000'`,
  second: (date) => `TO_VARCHAR(${date}, 'yyyy-MM-dd"T"HH:MM:SS') || '.000'`,

  month: (date) => `TO_VARCHAR(${date}, 'YYYY-MM') || '-01T00:00:00.000'`,
  quarter: (date) => `SUBSTR_BEFORE(QUARTER(${date}), 'Q') || MAP(SUBSTR_AFTER(QUARTER(${date}), 'Q'), 1, '01', 2, '04', 3, '07', 4, '10') || '-01T00:00:00.000'`,
  year: (date) => `TO_VARCHAR(${date}, 'YYYY') || '-01-01T00:00:00.000'`,
};

class HanaFilter extends BaseFilter {
  likeIgnoreCase(column, not, param, type) {
    const p = (!type || type === 'contains' || type === 'ends') ? '%' : '';
    const s = (!type || type === 'contains' || type === 'starts') ? '%' : '';
    return `${column}${not ? ' NOT' : ''} LIKE CONCAT('${p}', ${this.allocateParam(param)}, '${s}')`;
  }
}

export class HanaQuery extends BaseQuery {
  newFilter(filter) {
    return new HanaFilter(this, filter);
  }

  /**
   * SAP HANA doesn't support group by index,
   * using forSelect dimensions for grouping
   */
  groupByClause() {
    const dimensions = this.forSelect().filter(item => !!item.dimension);
    if (!dimensions.length) {
      return '';
    }
    return ` GROUP BY ${dimensions.map(item => item.dimensionSql()).join(', ')}`;
  }

  convertTz(field) {
    return `UTCTOLOCAL(TO_TIMESTAMP(${field}), 'UTC')`;
  }

  timeStampCast(value) {
    return `TO_TIMESTAMP(${value})`;
  }

  timestampFormat() {
    return moment.HTML5_FMT.DATETIME_LOCAL_MS;
  }

  dateTimeCast(value) {
    return `TO_TIMESTAMP(${value})`;
  }

  subtractInterval(date, interval) {
    return `ADD_DAYS(${date}, -${interval})`;
  }

  addInterval(date, interval) {
    return `ADD_DAYS(${date}, ${interval})`;
  }

  timeGroupedColumn(granularity, dimension) {
    return `TO_TIMESTAMP(${GRANULARITY_TO_INTERVAL[granularity](dimension)})`;
  }

  escapeColumnName(name) {
    return `"${name}"`;
  }

  seriesSql(timeDimension) {
    const values = timeDimension.timeSeries().map(
      ([from, to]) => `select '${from}' f, '${to}' t`
    ).join(' UNION ALL ');
    return `SELECT TO_TIMESTAMP(dates.f) date_from, TO_TIMESTAMP(dates.t) date_to FROM (${values}) AS dates`;
  }

  concatStringsSql(strings) {
    return `CONCAT(${strings.join(', ')})`;
  }

  unixTimestampSql() {
    return 'UNIX_TIMESTAMP()';
  }

  wrapSegmentForDimensionSelect(sql) {
    return `IF(${sql}, 1, 0)`;
  }

  preAggregationLoadSql(cube, preAggregation, tableName) {
    const sqlAndParams = this.preAggregationSql(cube, preAggregation);
    return [`CREATE TABLE ${tableName} ${this.asSyntaxTable} ( ${sqlAndParams[0]} )`, sqlAndParams[1]];
  }
}
