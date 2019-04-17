const BaseQuery = require('./BaseQuery');
const BaseFilter = require('./BaseFilter');

const GRANULARITY_TO_INTERVAL = {
  date: 'DAY',
  week: 'WEEK(MONDAY)',
  hour: 'HOUR',
  month: 'MONTH',
  year: 'YEAR'
};

class BigqueryFilter extends BaseFilter {
  likeIgnoreCase(column, not) {
    return `LOWER(${column})${not ? ' NOT' : ''} LIKE CONCAT('%', LOWER(?) ,'%')`;
  }

  castParameter() {
    if (this.definition().type === 'boolean') {
      return 'CAST(? AS BOOL)';
    } else if (this.measure || this.definition().type === 'number') {
      // TODO here can be measure type of string actually
      return 'CAST(? AS FLOAT64)';
    }
    return '?';
  }
}

class BigqueryQuery extends BaseQuery {
  convertTz(field) {
    return `DATETIME(${field}, '${this.timezone}')`;
  }

  timeStampCast(value) {
    return `TIMESTAMP(${value})`;
  }

  dateTimeCast(value) {
    return `DATETIME(TIMESTAMP(${value}))`;
  }

  escapeColumnName(name) {
    return `\`${name}\``;
  }

  timeGroupedColumn(granularity, dimension) {
    return `DATETIME_TRUNC(${dimension}, ${GRANULARITY_TO_INTERVAL[granularity]})`;
  }

  newFilter(filter) {
    return new BigqueryFilter(this, filter);
  }

  aliasName(name) {
    return super.aliasName(name).replace(/\./g, '_');
  }

  dateSeriesSql(timeDimension) {
    return `${timeDimension.dateSeriesAliasName()} AS (${this.seriesSql(timeDimension)})`;
  }

  seriesSql(timeDimension) {
    const values = timeDimension.timeSeries().map(
      ([from, to]) => `select '${from}' f, '${to}' t`
    ).join(' UNION ALL ');
    return `SELECT ${this.dateTimeCast('dates.f')} date_from, ${this.dateTimeCast('dates.t')} date_to FROM (${values}) AS dates`;
  }

  overTimeSeriesSelect(cumulativeMeasures, dateSeriesSql, baseQuery, dateJoinConditionSql, baseQueryAlias) {
    const forSelect = this.dateSeriesSelect().concat(
      this.dimensions.concat(cumulativeMeasures).map(s => s.cumulativeSelectColumns())
    ).filter(c => !!c).join(', ');
    const outerSeriesAlias = this.cubeAlias('outer_series');
    const outerBase = this.cubeAlias('outer_base');
    const timeDimensionAlias = this.timeDimensions.map(d => d.aliasName()).filter(d => !!d)[0];
    const aliasesForSelect = this.timeDimensions.map(d => d.dateSeriesSelectColumn(outerSeriesAlias))
      .concat(
      this.dimensions.concat(cumulativeMeasures).map(s => s.aliasName())
    ).filter(c => !!c).join(', ');
    const dateSeriesAlias = this.timeDimensions.map(d => `${d.dateSeriesAliasName()}`).filter(c => !!c)[0];
    return `
    WITH ${dateSeriesSql} SELECT ${aliasesForSelect} FROM
    ${dateSeriesAlias} ${outerSeriesAlias}
    LEFT JOIN (
      SELECT ${forSelect} FROM ${dateSeriesAlias}
      INNER JOIN (${baseQuery}) AS ${baseQueryAlias} ON ${dateJoinConditionSql}
      ${this.groupByClause()}
    ) AS ${outerBase} ON ${outerSeriesAlias}.${this.escapeColumnName('date_from')} = ${outerBase}.${timeDimensionAlias}
    `;
  }

  subtractInterval(date, interval) {
    return `DATETIME_SUB(${date}, INTERVAL ${interval})`;
  }

  addInterval(date, interval) {
    return `DATETIME_ADD(${date}, INTERVAL ${interval})`;
  }

  nowTimestampSql() {
    return `CURRENT_TIMESTAMP()`;
  }

  preAggregationLoadSql(cube, preAggregation, tableName) {
    return this.preAggregationSql(cube, preAggregation);
  }

  hllInit(sql) {
    return `HLL_COUNT.INIT(${sql})`;
  }

  hllMerge(sql) {
    return `HLL_COUNT.MERGE(${sql})`;
  }

  countDistinctApprox(sql) {
    return `APPROX_COUNT_DISTINCT(${sql})`;
  }

  concatStringsSql(strings) {
    return `CONCAT(${strings.join(", ")})`;
  }
}

module.exports = BigqueryQuery;
