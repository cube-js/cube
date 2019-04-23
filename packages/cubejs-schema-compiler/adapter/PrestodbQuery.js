const BaseQuery = require('./BaseQuery');
const BaseFilter = require('./BaseFilter');

const GRANULARITY_TO_INTERVAL = {
  date: 'day',
  week: 'week',
  hour: 'hour',
  month: 'month',
  year: 'year'
};

class PrestodbFilter extends BaseFilter {
  likeIgnoreCase(column, not) {
    return `LOWER(${column})${not ? ' NOT' : ''} LIKE CONCAT('%', LOWER(?) ,'%')`;
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
    return `from_iso8601_timestamp(${value})`;
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

  subtractInterval(date, interval) {
    const [intervalValue, intervalUnit] = interval.split(" ");
    return `${date} - interval '${intervalValue}' ${intervalUnit}`;
  }

  addInterval(date, interval) {
    const [intervalValue, intervalUnit] = interval.split(" ");
    return `${date} + interval '${intervalValue}' ${intervalUnit}`;
  }

  seriesSql(timeDimension) {
    const values = timeDimension.timeSeries().map(
      ([from, to]) => `select '${from}' f, '${to}' t`
    ).join(' UNION ALL ');
    return `SELECT from_iso8601_timestamp(dates.f) date_from, from_iso8601_timestamp(dates.t) date_to FROM (${values}) AS dates`;
  }
}

module.exports = PrestodbQuery;
