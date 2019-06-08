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

  escapeColumnName(name) {
    return `"${name}"`;
  }

  cubeAlias(cubeName) {
    const prefix = this.safeEvaluateSymbolContext().cubeAliasPrefix || this.cubeAliasPrefix;
    return this.escapeColumnName(this.aliasName(`${prefix ? prefix + '__' : ''}${cubeName}`));
  }

  convertTz(field) {
    return `(${field}::timestamptz AT TIME ZONE '${this.timezone}')`;
  }

  timeGroupedColumn(granularity, dimension) {
    return `date_trunc('${GRANULARITY_TO_INTERVAL[granularity]}', ${dimension})`;
  }
}

module.exports = PostgresQuery;
