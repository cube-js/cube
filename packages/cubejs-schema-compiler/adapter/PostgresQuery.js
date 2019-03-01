const BaseQuery = require('./BaseQuery');
const ParamAllocator = require('./ParamAllocator');

const GRANULARITY_TO_INTERVAL = {
  date: 'day',
  week: 'week',
  hour: 'hour',
  month: 'month',
  year: 'year'
};

class PostgresParamAllocator extends ParamAllocator {
  paramPlaceHolder(paramIndex) {
    return `$${paramIndex + 1}`;
  }
}

class PostgresQuery extends BaseQuery {
  newParamAllocator() {
    return new PostgresParamAllocator();
  }

  convertTz(field) {
    return `(${field}::timestamptz AT TIME ZONE '${this.timezone}')`;
  }

  timeGroupedColumn(granularity, dimension) {
    return `date_trunc('${GRANULARITY_TO_INTERVAL[granularity]}', ${dimension})`;
  }
}

module.exports = PostgresQuery;
