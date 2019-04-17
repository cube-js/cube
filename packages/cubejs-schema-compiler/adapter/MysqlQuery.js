const moment = require('moment-timezone');

const BaseQuery = require('./BaseQuery');
const BaseFilter = require('./BaseFilter');

const GRANULARITY_TO_INTERVAL = {
  date: (date) => `DATE_FORMAT(${date}, '%Y-%m-%dT00:00:00.000')`,
  week: (date) => `DATE_FORMAT(date_add('1900-01-01', interval TIMESTAMPDIFF(WEEK, '1900-01-01', ${date}) WEEK), '%Y-%m-%dT00:00:00.000')`,
  hour: (date) => `DATE_FORMAT(${date}, '%Y-%m-%dT%H:00:00.000')`,
  month: (date) => `DATE_FORMAT(${date}, '%Y-%m-01T00:00:00.000')`,
  year: (date) => `DATE_FORMAT(${date}, '%Y-01-01T00:00:00.000')`
};

class MysqlFilter extends BaseFilter {
  likeIgnoreCase(column, not) {
    return `${column}${not ? ' NOT' : ''} LIKE CONCAT('%', ?, '%')`;
  }
}

class MysqlQuery extends BaseQuery {
  newFilter(filter) {
    return new MysqlFilter(this, filter);
  }

  convertTz(field) {
    return `CONVERT_TZ(${field}, @@session.time_zone, '${moment().tz(this.timezone).format('Z')}')`;
  }

  timeStampCast(value) {
    return `TIMESTAMP(convert_tz(${value}, '+00:00', @@session.time_zone))`;
  }

  dateTimeCast(value) {
    return `TIMESTAMP(${value})`;
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
    return `\`${name}\``;
  }

  seriesSql(timeDimension) {
    const values = timeDimension.timeSeries().map(
      ([from, to]) => `select '${from}' f, '${to}' t`
    ).join(' UNION ALL ');
    return `SELECT TIMESTAMP(dates.f) date_from, TIMESTAMP(dates.t) date_to FROM (${values}) AS dates`;
  }
}

module.exports = MysqlQuery;
