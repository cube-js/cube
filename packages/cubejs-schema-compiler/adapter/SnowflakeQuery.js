const BaseQuery = require('./BaseQuery');

const GRANULARITY_TO_INTERVAL = {
  date: 'DAY',
  week: 'WEEK',
  hour: 'HOUR',
  month: 'MONTH',
  year: 'YEAR'
};

class SnowflakeQuery extends BaseQuery {
  convertTz(field) {
    return `CONVERT_TIMEZONE('${this.timezone}', ${field}::timestamp_tz)::timestamp_ntz`;
  }

  timeGroupedColumn(granularity, dimension) {
    return `date_trunc('${GRANULARITY_TO_INTERVAL[granularity]}', ${dimension})`;
  }

  timeStampCast(value) {
    return `${value}::timestamp_tz`;
  }
}

module.exports = SnowflakeQuery;
