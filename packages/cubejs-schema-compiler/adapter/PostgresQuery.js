const BaseQuery = require('./BaseQuery');
const ParamAllocator = require('./ParamAllocator');

const GRANULARITY_TO_INTERVAL = {
  day: 'day',
  week: 'week',
  hour: 'hour',
  minute: 'minute',
  second: 'second',
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

  hllInit(sql) {
    return `hll_add_agg(hll_hash_any(${sql}))`;
  }

  hllMerge(sql) {
    return `round(hll_cardinality(hll_union_agg(${sql})))`;
  }

  countDistinctApprox(sql) {
    return `round(hll_cardinality(hll_add_agg(hll_hash_any(${sql}))))`;
  }
}

module.exports = PostgresQuery;
