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
  day: (date) => `FORMAT(${date}, 'yyyy-MM-ddT00:00:00.000')`,
  week: (date) => `FORMAT(dateadd(week, DATEDIFF(week, '1900-01-01', ${date}), '1900-01-01'), 'yyyy-MM-ddT00:00:00.000')`,
  hour: (date) => `FORMAT(${date}, 'yyyy-MM-ddTHH:00:00.000')`,
  minute: (date) => `FORMAT(${date}, 'yyyy-MM-ddTHH:mm:00.000')`,
  second: (date) => `FORMAT(${date}, 'yyyy-MM-ddTHH:mm:ss.000')`,
  month: (date) => `FORMAT(${date}, 'yyyy-MM-01T00:00:00.000')`,
  year: (date) => `FORMAT(${date}, 'yyyy-01-01T00:00:00.000')`
};

class MssqlFilter extends BaseFilter {
  // noinspection JSMethodCanBeStatic
  escapeWildcardChars(param) {
    return typeof param === 'string' ? param.replace(/([_%])/gi, '[$1]') : param;
  }

  likeIgnoreCase(column, not) {
    return `LOWER(${column})${not ? ' NOT' : ''} LIKE CONCAT('%', LOWER(?) ,'%')`;
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
    return this.offset ? ` OFFSET ${parseInt(this.offset, 10)} ROWS` : '';
  }

  topLimit() {
    return this.rowLimit === null ? '' : ` TOP ${this.rowLimit && parseInt(this.rowLimit, 10) || 10000}`;
  }

  groupByClause() {
    const dimensionsForSelect = this.dimensionsForSelect();
    const dimensionColumns = R.flatten(dimensionsForSelect.map(s => s.selectColumns() && s.dimensionSql()))
      .filter(s => !!s);
    return dimensionColumns.length ? ` GROUP BY ${dimensionColumns.join(', ')}` : '';
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
}

module.exports = MssqlQuery;
