const { BaseFilter, BaseQuery } = require('@cubejs-backend/schema-compiler');
const { parseSqlInterval } = require('@cubejs-backend/shared');

const GRANULARITY_TO_INTERVAL = {
  week: (date) => `DATE_TRUNC('week', ${date})`,
  second: (date) => `DATE_TRUNC('second', ${date})`,
  minute: (date) => `DATE_TRUNC('minute', ${date})`,
  hour: (date) => `DATE_TRUNC('hour', ${date})`,
  day: (date) => `DATE_TRUNC('day', ${date})`,
  month: (date) => `DATE_TRUNC('month', ${date})`,
  quarter: (date) => `DATE_TRUNC('quarter', ${date})`,
  year: (date) => `DATE_TRUNC('year', ${date})`
};

class DremioFilter extends BaseFilter {
  likeIgnoreCase(column, not, param, type) {
    const p = (!type || type === 'contains' || type === 'ends') ? '%' : '';
    const s = (!type || type === 'contains' || type === 'starts') ? '%' : '';
    return ` ILIKE (${column}${not ? ' NOT' : ''}, CONCAT('${p}', ${this.allocateParam(param)}, '${s}'))`;
  }

  castParameter() {
    if (this.definition().type === 'boolean') {
      return 'CAST(? AS BOOLEAN)';
    } else if (this.measure || this.definition().type === 'number') {
      // TODO here can be measure type of string actually
      return 'CAST(? AS DOUBLE)';
    }

    return '?';
  }
}

class DremioQuery extends BaseQuery {
  newFilter(filter) {
    return new DremioFilter(this, filter);
  }

  /**
   * CONVERT_TIMEZONE([sourceTimezone string], destinationTimezone string,
   *    timestamp date, timestamp, or string in ISO 8601 format) → timestamp
   * sourceTimezone (optional): The time zone of the timestamp. If you omit this parameter,
   *    Dremio assumes that the source time zone is UTC.
   * @see https://docs.dremio.com/cloud/reference/sql/sql-functions/functions/CONVERT_TIMEZONE/
   */
  convertTz(field) {
    return `CONVERT_TIMEZONE('${this.timezone}', ${field})`;
  }

  timeStampCast(value) {
    return `TO_TIMESTAMP(${value}, 'YYYY-MM-DD"T"HH24:MI:SS.FFF')`;
  }

  timestampFormat() {
    return 'YYYY-MM-DDTHH:mm:ss.SSS';
  }

  dateTimeCast(value) {
    return `TO_TIMESTAMP(${value})`;
  }

  subtractInterval(date, interval) {
    const formattedTimeIntervals = this.formatInterval(interval);
    const intervalFormatted = formattedTimeIntervals[0];
    const timeUnit = formattedTimeIntervals[1];
    return `DATE_SUB(${date}, CAST(${intervalFormatted} as INTERVAL ${timeUnit}))`;
  }

  addInterval(date, interval) {
    const formattedTimeIntervals = this.formatInterval(interval);
    const intervalFormatted = formattedTimeIntervals[0];
    const timeUnit = formattedTimeIntervals[1];
    return `DATE_ADD(${date}, CAST(${intervalFormatted} as INTERVAL ${timeUnit}))`;
  }

  timeGroupedColumn(granularity, dimension) {
    return GRANULARITY_TO_INTERVAL[granularity](dimension);
  }

  escapeColumnName(name) {
    return `"${name}"`;
  }

  seriesSql(timeDimension) {
    const values = timeDimension.timeSeries().map(
      ([from, to]) => `select '${from}' f, '${to}' t`
    ).join(' UNION ALL ');
    return `SELECT TO_TIMESTAMP(dates.f, 'YYYY-MM-DDTHH:MI:SS.FFF') date_from, TO_TIMESTAMP(dates.t, 'YYYY-MM-DDTHH:MI:SS.FFF') date_to FROM (${values}) AS dates`;
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
  
  /**
   * The input interval with (possible) plural units, like "1 hour 2 minutes", "2 year", "3 months", "4 weeks", "5 days", "3 months 24 days 15 minutes", ...
   * will be converted to Dremio dialect.
   * @see https://docs.dremio.com/24.3.x/reference/sql/sql-functions/functions/DATE_ADD/
   * @see https://docs.dremio.com/24.3.x/reference/sql/sql-functions/functions/DATE_SUB/
   * It returns a tuple of (formatted interval, timeUnit to use in date functions)
   * This function only supports the following scenarios for now: 
   *   ie. n year[s] or n month[3] or n day[s] 
   */  
  formatInterval(interval) {
    const intervalParsed = parseSqlInterval(interval);
    const intKeys = Object.keys(intervalParsed).length;

    if (intervalParsed.year && intKeys === 1) {
      return [`${intervalParsed.year}`, 'YEAR'];
    } else if (intervalParsed.quarter && intKeys === 1) {
      // dremio interval does not support quarter. Convert to month
      return [`${intervalParsed.quarter * 3}`, 'MONTH'];
    } else if (intervalParsed.month && intKeys === 1) {
      return [`${intervalParsed.month}`, 'MONTH'];
    } else if (intervalParsed.month && intKeys === 1) {
      return [`${intervalParsed.day}`, 'DAY'];
    } 

    throw new Error(`Cannot transform interval expression "${interval}" to Dremio dialect`);
  }

}

module.exports = DremioQuery;
