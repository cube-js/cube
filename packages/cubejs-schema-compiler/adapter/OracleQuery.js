const BaseQuery = require('./BaseQuery');

const GRANULARITY_VALUE = {
  date: 'DD',
  week: 'IW',
  hour: 'HH24',
  month: 'MM',
  year: 'YYYY'
};

class OracleQuery extends BaseQuery {
  /**
   * LIMIT on Oracle it's illegal
   */
  groupByDimensionLimit() {
    return this.rowLimit === null ? '' : ` FETCH NEXT ${this.rowLimit && parseInt(this.rowLimit, 10) || 10000} ROWS ONLY`;
  }

  /**
   * "AS" for table aliasing on Oracle it's illegal
   * ORA-00933: SQL command not properly ended
   */
  asSyntaxTable() {
    return '';
  }

  asSyntaxJoin() {
    return this.asSyntaxTable();
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
    return timeDimension.dateFieldType() === 'string' ? '?' : this.timeStampCast('?');
  }

  timeGroupedColumn(granularity, dimension) {
    if (!granularity) {
      return dimension;
    }
    return `TRUNC(${dimension}, '${GRANULARITY_VALUE[granularity]}')`;
  }
};

module.exports = OracleQuery;
