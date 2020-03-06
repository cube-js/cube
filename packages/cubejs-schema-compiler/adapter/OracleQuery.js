const BaseQuery = require('./BaseQuery');
const BaseFilter = require('./BaseFilter');

const GRANULARITY_VALUE = {
  day: 'DD',
  week: 'IW',
  hour: 'HH24',
  minute: 'mm',
  second: 'ss',
  month: 'MM',
  year: 'YYYY'
};

class OracleFilter extends BaseFilter {
  castParameter() {
    return ':"?"';
  }

  /**
   * "ILIKE" does't support
   */
  likeIgnoreCase(column, not) {
    return `${column}${not ? ' NOT' : ''} LIKE '%' || ${this.castParameter()} || '%'`;
  }
}

class OracleQuery extends BaseQuery {
  /**
   * "LIMIT" on Oracle it's illegal
   */
  groupByDimensionLimit() {
    const limitClause = this.rowLimit === null ? '' : ` FETCH NEXT ${this.rowLimit && parseInt(this.rowLimit, 10) || 10000} ROWS ONLY`;
    const offsetClause = this.offset ? ` OFFSET ${parseInt(this.offset, 10)} ROWS` : '';
    return `${offsetClause}${limitClause}`;
  }

  /**
   * "AS" for table aliasing on Oracle it's illegal
   */
  get asSyntaxTable() {
    return '';
  }

  get asSyntaxJoin() {
    return this.asSyntaxTable;
  }

  /**
   * Oracle doesn't support group by index,
   * using forSelect dimensions for grouping
   */
  groupByClause() {
    const dimensions = this.forSelect().filter(item => !!item.dimension);
    if (!dimensions.length) {
      return '';
    }
    return ` GROUP BY ${dimensions.map(item => item.dimensionSql()).join(", ")}`;
  }

  convertTz(field) {
    /**
     * TODO: add offset timezone
     */
    return field;
  }

  dateTimeCast(value) {
    return `to_date(:"${value}", 'YYYY-MM-DD"T"HH24:MI:SS"Z"')`;
  }

  timeStampCast(value) {
    return this.dateTimeCast(value);
  }

  timeStampParam(timeDimension) {
    return timeDimension.dateFieldType() === 'string' ? ':"?"' : this.timeStampCast('?');
  }

  timeGroupedColumn(granularity, dimension) {
    if (!granularity) {
      return dimension;
    }
    return `TRUNC(${dimension}, '${GRANULARITY_VALUE[granularity]}')`;
  }

  newFilter(filter) {
    return new OracleFilter(this, filter);
  }

  unixTimestampSql() {
    return `((cast (systimestamp at time zone 'UTC' as date) - date '1970-01-01') * 86400)`;
  }
}

module.exports = OracleQuery;
