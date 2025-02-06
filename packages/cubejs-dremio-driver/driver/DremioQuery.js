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
    return `TO_TIMESTAMP(${value}, 'YYYY-MM-DD"T"HH24:MI:SS.FFF')`;
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

  /**
   * @param {string} timestamp
   * @param {string} interval
   * @returns {string}
   */
  addTimestampInterval(timestamp, interval) {
    return this.addInterval(timestamp, interval);
  }

  /**
   * @param {string} timestamp
   * @param {string} interval
   * @returns {string}
   */
  subtractTimestampInterval(timestamp, interval) {
    return this.subtractInterval(timestamp, interval);
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

    return `SELECT TO_TIMESTAMP(dates.f, 'YYYY-MM-DD"T"HH24:MI:SS.FFF') date_from, TO_TIMESTAMP(dates.t, 'YYYY-MM-DD"T"HH24:MI:SS.FFF') date_to FROM (${values}) AS dates`;
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
   *   ie. n year[s] or n quarter[s] or n month[s] or n week[s] or n day[s]
   */
  formatInterval(interval) {
    const intervalParsed = parseSqlInterval(interval);
    const intKeys = Object.keys(intervalParsed).length;

    if (intervalParsed.year && intKeys === 1) {
      return [`${intervalParsed.year}`, 'YEAR'];
    } else if (intervalParsed.quarter && intKeys === 1) {
      // dremio interval does not support quarter. Convert to month
      return [`${intervalParsed.quarter * 3}`, 'MONTH'];
    } else if (intervalParsed.week && intKeys === 1) {
      // dremio interval does not support week. Convert to days
      return [`${intervalParsed.week * 7}`, 'DAY'];
    } else if (intervalParsed.month && intKeys === 1) {
      return [`${intervalParsed.month}`, 'MONTH'];
    } else if (intervalParsed.month && intKeys === 1) {
      return [`${intervalParsed.day}`, 'DAY'];
    } else if (intervalParsed.hour && intKeys === 1) {
      return [`${intervalParsed.hour}`, 'HOUR'];
    } else if (intervalParsed.minute && intKeys === 1) {
      return [`${intervalParsed.minute}`, 'MINUTE'];
    } else if (intervalParsed.second && intKeys === 1) {
      return [`${intervalParsed.second}`, 'SECOND'];
    }

    throw new Error(`Cannot transform interval expression "${interval}" to Dremio dialect`);
  }

  sqlTemplates() {
    const templates = super.sqlTemplates();
    templates.functions.CURRENTDATE = 'CURRENT_DATE';
    templates.functions.DATETRUNC = 'DATE_TRUNC(\'{{ date_part }}\', {{ args_concat }})';
    templates.functions.DATEPART = 'DATE_PART(\'{{ date_part }}\', {{ args_concat }})';
    // really need the date locale formatting here...
    templates.functions.DATE = 'TO_DATE({{ args_concat }},\'YYYY-MM-DD\', 1)';
    templates.functions.DATEDIFF = 'DATE_DIFF(DATE, DATE_TRUNC(\'{{ date_part }}\', {{ args[1] }}), DATE_TRUNC(\'{{ date_part }}\', {{ args[2] }}))';
    templates.expressions.interval_single_date_part = 'CAST({{ num }} as INTERVAL {{ date_part }})';
    templates.quotes.identifiers = '"';
    return templates;
  }
}

module.exports = DremioQuery;
