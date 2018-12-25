const BaseQuery = require('./BaseQuery');
const BaseFilter = require('./BaseFilter');

const GRANULARITY_TO_INTERVAL = {
  date: 'day',
  week: 'week',
  hour: 'hour',
  month: 'month'
};

class PrestodbFilter extends BaseFilter {
  containsWhere(column) {
    return `LOWER(${column}) LIKE CONCAT('%', LOWER(?) ,'%')`;
  }

  notContainsWhere(column) {
    return `LOWER(${column}) NOT LIKE CONCAT('%', LOWER(?) ,'%') OR ${column} IS NULL`;
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

class PrestodbQuery extends BaseQuery {
  newFilter(filter) {
    return new PrestodbFilter(this, filter);
  }

  timeStampParam() {
    return `from_iso8601_timestamp(?)`;
  }

  timeStampCast(value) {
    return `CAST(${value} as TIMESTAMP)`; // TODO
  }

  dateTimeCast(value) {
    return `CAST(${value} as TIMESTAMP)`; // TODO
  }

  convertTz(field) {
    const atTimezone = `${field} AT TIME ZONE '${this.timezone}'`;
    return this.timezone ?
      `CAST(date_add('minute', timezone_minute(${atTimezone}), date_add('hour', timezone_hour(${atTimezone}), ${atTimezone})) AS TIMESTAMP)` :
      field;
  }

  timeGroupedColumn(granularity, dimension) {
    return `date_trunc('${GRANULARITY_TO_INTERVAL[granularity]}', ${dimension})`;
  }
}

module.exports = PrestodbQuery;
