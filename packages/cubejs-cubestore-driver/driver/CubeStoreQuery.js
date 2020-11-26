const moment = require('moment-timezone');

const BaseQuery = require('@cubejs-backend/schema-compiler/adapter/BaseQuery');
const BaseFilter = require('@cubejs-backend/schema-compiler/adapter/BaseFilter');
const UserError = require('@cubejs-backend/schema-compiler/compiler/UserError');

const GRANULARITY_TO_INTERVAL = {
  day: 'day',
  week: 'week',
  hour: 'hour',
  minute: 'minute',
  second: 'second',
  month: 'month',
  year: 'year'
};

class CubeStoreFilter extends BaseFilter {
  likeIgnoreCase(column, not, param) {
    return `${column}${not ? ' NOT' : ''} LIKE CONCAT('%', ${this.allocateParam(param)}, '%')`;
  }
}

class CubeStoreQuery extends BaseQuery {
  newFilter(filter) {
    return new CubeStoreFilter(this, filter);
  }

  convertTz(field) {
    return `CONVERT_TZ(${field}, '${moment().tz(this.timezone).format('Z')}')`;
  }

  timeStampParam() {
    return `to_timestamp(?)`;
  }

  timeStampCast(value) {
    return `CAST(${value} as TIMESTAMP)`; // TODO
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
    return `date_trunc(${dimension}, '${GRANULARITY_TO_INTERVAL[granularity]}')`;
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

module.exports = CubeStoreQuery;
