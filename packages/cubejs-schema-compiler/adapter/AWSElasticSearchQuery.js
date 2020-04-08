// const moment = require('moment-timezone');
const R = require('ramda');

const BaseQuery = require('./BaseQuery');
const BaseFilter = require('./BaseFilter');

const GRANULARITY_TO_INTERVAL = {
  day: (date) => `DATE_FORMAT(${date}, 'yyyy-MM-dd 00:00:00.000')`,
  // eslint-disable-next-line no-unused-vars
  week: (date) => { throw new Error('Week is unsupported'); }, // TODO
  hour: (date) => `DATE_FORMAT(${date}, 'yyyy-MM-dd HH:00:00.000')`,
  minute: (date) => `DATE_FORMAT(${date}, 'yyyy-MM-dd HH:mm:00.000')`,
  second: (date) => `DATE_FORMAT(${date}, 'yyyy-MM-dd HH:mm:ss.000')`,
  month: (date) => `DATE_FORMAT(${date}, 'yyyy-MM-01 00:00:00.000')`,
  year: (date) => `DATE_FORMAT(${date}, 'yyyy-01-01 00:00:00.000')`
};

class AWSElasticSearchQueryFilter extends BaseFilter {
  likeIgnoreCase(column, not) {
    return `${column}${not ? ' NOT' : ''} LIKE CONCAT('%', ?, '%')`;
  }
}

class AWSElasticSearchQuery extends BaseQuery {
  newFilter(filter) {
    return new AWSElasticSearchQueryFilter(this, filter);
  }

  convertTz(field) {
    return `${field}`; // TODO
  }

  timeStampCast(value) {
    return `${value}`;
  }

  dateTimeCast(value) {
    return `${value}`; // TODO
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

  groupByClause() {
    const dimensionsForSelect = this.dimensionsForSelect();
    const dimensionColumns = R.flatten(dimensionsForSelect.map(s => s.selectColumns() && s.dimensionSql()))
      .filter(s => !!s);
    return dimensionColumns.length ? ` GROUP BY ${dimensionColumns.join(', ')}` : '';
  }

  orderHashToString(hash) {
    if (!hash || !hash.id) {
      return null;
    }

    const fieldAlias = this.getFieldAlias(hash.id);

    if (fieldAlias === null) {
      return null;
    }

    const direction = hash.desc ? 'DESC' : 'ASC';
    return `${fieldAlias} ${direction}`;
  }

  getFieldAlias(id) {
    const equalIgnoreCase = (a, b) => (
      typeof a === 'string' && typeof b === 'string' && a.toUpperCase() === b.toUpperCase()
    );

    let field;

    field = this.dimensionsForSelect().find(d => equalIgnoreCase(d.dimension, id));

    if (field) {
      return field.dimensionSql();
    }

    field = this.measures.find(d => equalIgnoreCase(d.measure, id) || equalIgnoreCase(d.expressionName, id));

    if (field) {
      return field.aliasName(); // TODO isn't supported
    }

    return null;
  }

  escapeColumnName(name) {
    return `${name}`; // TODO
  }
}

module.exports = AWSElasticSearchQuery;
