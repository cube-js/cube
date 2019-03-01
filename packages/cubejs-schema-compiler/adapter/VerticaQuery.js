const BaseQuery = require('./BaseQuery');

const GRANULARITY_TO_INTERVAL = {
  date: 'DD',
  week: 'W',
  hour: 'HH24',
  month: 'MM',
  year: 'YY'
};

class VerticaQuery extends BaseQuery {
  convertTz(field) {
    return `${field} AT TIME ZONE '${this.timezone}'`;
  }

  timeStampParam(timeDimension) {
    return this.timeStampCast(`?`);
  }

  timeGroupedColumn(granularity, dimension) {
    return `TRUNC(${dimension}, '${GRANULARITY_TO_INTERVAL[granularity]}')`;
  }
}

module.exports = VerticaQuery;
