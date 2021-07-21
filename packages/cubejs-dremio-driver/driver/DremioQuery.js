const moment = require('moment-timezone');
const { BaseFilter, BaseQuery } = require('@cubejs-backend/schema-compiler');

const GRANULARITY_TO_INTERVAL = {
  week: (date) => `DATE_TRUNC('week', ${date})`,
  second: (date) => `DATE_TRUNC('second', ${date})`,
  minute: (date) => `DATE_TRUNC('minute', ${date})`,
  hour: (date) => `DATE_TRUNC('hour', ${date})`,
  day: (date) => `DATE_TRUNC('day', ${date})`,
  month: (date) => `DATE_TRUNC('month', ${date})`,
  year: (date) => `DATE_TRUNC('year', ${date})`
};

class DremioFilter extends BaseFilter {
  likeIgnoreCase(column, not, param) {
    return ` ILIKE (${column}${not ? ' NOT' : ''}, CONCAT('%', ${this.allocateParam(param)}, '%'))`;
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

  convertTz(field) {
    const targetTZ = moment().tz(this.timezone).format('Z');
    return `CONVERT_TIMEZONE('${targetTZ}', ${field})`;
  }

  timeStampCast(value) {
    return `TO_TIMESTAMP(${value}, 'YYYY-MM-DD"T"HH24:MI:SS.FFF')`;
  }

  timestampFormat() {
    return moment.HTML5_FMT.DATETIME_LOCAL_MS;
  }

  dateTimeCast(value) {
    return `TO_TIMESTAMP(${value})`;
  }

  subtractInterval(date, interval) {
    return `DATE_SUB(${date}, INTERVAL ${interval})`;
  }

  addInterval(date, interval) {
    return `DATE_ADD(${date}, INTERVAL ${interval})`;
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
    return `SELECT TIMESTAMP(dates.f) date_from, TIMESTAMP(dates.t) date_to FROM (${values}) AS dates`;
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
}

module.exports = DremioQuery;
