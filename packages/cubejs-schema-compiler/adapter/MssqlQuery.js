const moment = require('moment-timezone');
const R = require('ramda');

const abbrs = {
  EST: 'Eastern Standard Time',
  EDT: 'Eastern Standard Time',
  CST: 'Central Standard Time',
  CDT: 'Central Standard Time',
  MST: 'Mountain Standard Time',
  MDT: 'Mountain Standard Time',
  PST: 'Pacific Standard Time',
  PDT: 'Pacific Standard Time',
};

moment.fn.zoneName = () => {
  const abbr = this.zoneAbbr();
  return abbrs[abbr] || abbr;
};

const BaseQuery = require('./BaseQuery');
const BaseFilter = require('./BaseFilter');
const ParamAllocator = require('./ParamAllocator');

class MssqlParamAllocator extends ParamAllocator {
  paramPlaceHolder(paramIndex) {
    return `@_${paramIndex + 1}`;
  }
}

const GRANULARITY_TO_INTERVAL = {
  day: (date) => `dateadd(day, DATEDIFF(day, 0, ${date}), 0)`,
  week: (date) => `dateadd(week, DATEDIFF(week, 0, ${date}), 0)`,
  hour: (date) => `dateadd(hour, DATEDIFF(hour, 0, ${date}), 0)`,
  minute: (date) => `dateadd(minute, DATEDIFF(minute, 0, ${date}), 0)`,
  second: (date) => `CAST(FORMAT(${date}, 'yyyy-MM-ddTHH:mm:ss.000') AS datetime)`, // until SQL 2016, this causes an int overflow; in SQL 2016 these calls can be changed to DATEDIFF_BIG
  month: (date) => `dateadd(month, DATEDIFF(month, 0, ${date}), 0)`,
  year: (date) => `dateadd(year, DATEDIFF(year, 0, ${date}), 0)`,
};

class MssqlFilter extends BaseFilter {
  // noinspection JSMethodCanBeStatic
  escapeWildcardChars(param) {
    return typeof param === 'string' ? param.replace(/([_%])/gi, '[$1]') : param;
  }

  likeIgnoreCase(column, not, param) {
    return `LOWER(${column})${not ? ' NOT' : ''} LIKE CONCAT('%', LOWER(${this.allocateParam(param)}) ,'%')`;
  }
}

class MssqlQuery extends BaseQuery {
  newFilter(filter) {
    return new MssqlFilter(this, filter);
  }

  convertTz(field) {
    return `TODATETIMEOFFSET(${field}, '${moment().tz(this.timezone).format('Z')}')`;
  }

  timeStampCast(value) {
    return `CAST(${value} AS DATETIME)`; // TODO
  }

  dateTimeCast(value) {
    return `CAST(${value} AS DATETIME)`;
  }

  timeGroupedColumn(granularity, dimension) {
    return GRANULARITY_TO_INTERVAL[granularity](dimension);
  }

  newParamAllocator() {
    return new MssqlParamAllocator();
  }

  groupByDimensionLimit() {
    if (this.rowLimit) {
      return this.offset ? ` OFFSET ${parseInt(this.offset, 10)} ROWS FETCH NEXT ${parseInt(this.rowLimit, 10)} ROWS ONLY` : '';
    } else {
      return this.offset ? ` OFFSET ${parseInt(this.offset, 10)} ROWS` : '';
    }
  }

  topLimit() {
    if (this.offset) {
      return '';
    }
    return this.rowLimit === null ? '' : ` TOP ${this.rowLimit && parseInt(this.rowLimit, 10) || 10000}`;
  }

  groupByClause() {
    const dimensionsForSelect = this.dimensionsForSelect();
    const dimensionColumns = R.flatten(dimensionsForSelect.map(s => s.selectColumns() && s.dimensionSql()))
      .filter(s => !!s);
    return dimensionColumns.length ? ` GROUP BY ${dimensionColumns.join(', ')}` : '';
  }

  overTimeSeriesSelect(cumulativeMeasures, dateSeriesSql, baseQuery, dateJoinConditionSql, baseQueryAlias) {
    const forGroupBy = this.timeDimensions.map(
      (t) => `${t.dateSeriesAliasName()}.${this.escapeColumnName('date_from')}`
    );
    const forSelect = this.dateSeriesSelect()
      .concat(this.dimensions.concat(cumulativeMeasures).map((s) => s.cumulativeSelectColumns()))
      .filter((c) => !!c)
      .join(', ');
    return (
      `SELECT ${forSelect} FROM ${dateSeriesSql}` +
      ` LEFT JOIN (${baseQuery}) ${this.asSyntaxJoin} ${baseQueryAlias} ON ${dateJoinConditionSql}` +
      ` GROUP BY ${forGroupBy}`
    );
  }

  nowTimestampSql() {
    return `CURRENT_TIMESTAMP`;
  }

  unixTimestampSql() {
    return `DATEDIFF(SECOND,'1970-01-01', GETUTCDATE())`;
  }

  preAggregationLoadSql(cube, preAggregation, tableName) {
    const sqlAndParams = this.preAggregationSql(cube, preAggregation);
    return [`SELECT * INTO ${tableName} FROM (${sqlAndParams[0]}) AS PreAggregation`, sqlAndParams[1]];
  }

  wrapSegmentForDimensionSelect(sql) {
    return `CAST((CASE WHEN ${sql} THEN 1 ELSE 0 END) AS BIT)`;
  }

  seriesSql(timeDimension) {
    const values = timeDimension.timeSeries().map(([from, to]) => `('${from}', '${to}')`);
    return `SELECT ${this.dateTimeCast('date_from')} date_from, ${this.dateTimeCast(
      'date_to'
    )} date_to FROM (VALUES ${values}) ${this.asSyntaxTable} dates (date_from, date_to)`;
  }
}

module.exports = MssqlQuery;
