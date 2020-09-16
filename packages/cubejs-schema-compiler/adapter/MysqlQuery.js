const moment = require('moment-timezone');

const BaseQuery = require('./BaseQuery');
const BaseFilter = require('./BaseFilter');
const UserError = require('../compiler/UserError');

const GRANULARITY_TO_INTERVAL = {
  day: (date) => `DATE_FORMAT(${date}, '%Y-%m-%dT00:00:00.000')`,
  week: (date) => `DATE_FORMAT(date_add('1900-01-01', interval TIMESTAMPDIFF(WEEK, '1900-01-01', ${date}) WEEK), '%Y-%m-%dT00:00:00.000')`,
  hour: (date) => `DATE_FORMAT(${date}, '%Y-%m-%dT%H:00:00.000')`,
  minute: (date) => `DATE_FORMAT(${date}, '%Y-%m-%dT%H:%i:00.000')`,
  second: (date) => `DATE_FORMAT(${date}, '%Y-%m-%dT%H:%i:%S.000')`,
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

  inDbTimeZone(date) {
    return this.inIntegrationTimeZone(date).clone().utc().format(moment.HTML5_FMT.DATETIME_LOCAL_MS);
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

  concatStringsSql(strings) {
    return `CONCAT(${strings.join(', ')})`;
  }

  unixTimestampSql() {
    return `UNIX_TIMESTAMP()`;
  }

  wrapSegmentForDimensionSelect(sql) {
    return `IF(${sql}, 1, 0)`;
  }

  preAggregationTableName(cube, preAggregationName, skipSchema) {
    const name = super.preAggregationTableName(cube, preAggregationName, skipSchema);
    if (name.length > 64) {
      throw new UserError(`MySQL can not work with table names that longer than 64 symbols. Consider using the 'sqlAlias' attribute in your cube and in your pre-aggregation definition for ${name}.`);
    }
    return name;
  }
}

module.exports = MysqlQuery;
