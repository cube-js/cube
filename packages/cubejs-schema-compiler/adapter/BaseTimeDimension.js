const momentRange = require('moment-range');
const moment = momentRange.extendMoment(require('moment-timezone'));

const BaseFilter = require('./BaseFilter');
const UserError = require('../compiler/UserError');

const TIME_SERIES = {
  date: (range) =>
    Array.from(range.by('day'))
      .map(d => [d.format('YYYY-MM-DDT00:00:00.000'), d.format('YYYY-MM-DDT23:59:59.999')]),
  month: (range) =>
    Array.from(range.snapTo('month').by('month'))
      .map(d => [d.format('YYYY-MM-01T00:00:00.000'), d.endOf('month').format('YYYY-MM-DDT23:59:59.999')]),
  year: (range) =>
    Array.from(range.snapTo('year').by('year'))
      .map(d => [d.format('YYYY-01-01T00:00:00.000'), d.endOf('year').format('YYYY-MM-DDT23:59:59.999')]),
  hour: (range) =>
    Array.from(range.by('hour'))
      .map(d => [d.format('YYYY-MM-DDTHH:00:00.000'), d.format('YYYY-MM-DDTHH:59:59.999')]),
  week: (range) =>
    Array.from(range.snapTo('isoweek').by('week'))
      .map(d => [d.startOf('isoweek').format('YYYY-MM-DDT00:00:00.000'), d.endOf('isoweek').format('YYYY-MM-DDT23:59:59.999')])
};

class BaseTimeDimension extends BaseFilter {
  constructor(query, timeDimension) {
    super(query, {
      dimension: timeDimension.dimension,
      operator: 'in_date_range',
      values: timeDimension.dateRange
    });
    this.query = query;
    this.dateRange = timeDimension.dateRange;
    this.granularity = timeDimension.granularity;
  }

  selectColumns() {
    if (!this.granularity) {
      return null;
    }
    return super.selectColumns();
  }

  aliasName() {
    if (!this.granularity) {
      return null;
    }
    return super.aliasName();
  }

  unescapedAliasName() {
    return `${this.query.aliasName(this.dimension)}_${this.granularity || 'date'}`; // TODO date here for rollups
  }

  dateSeriesAliasName() {
    return this.query.escapeColumnName(`${this.dimension}_series`);
  }

  dateSeriesSelectColumn(dateSeriesAliasName) {
    if (!this.granularity) {
      return null;
    }
    return `${dateSeriesAliasName || this.dateSeriesAliasName()}.${this.query.escapeColumnName('date_from')} ${this.aliasName()}`;
  }

  dimensionSql() {
    if (this.query.safeEvaluateSymbolContext().rollupQuery) {
      return super.dimensionSql();
    }
    if (this.query.safeEvaluateSymbolContext().ungrouped) {
      return this.convertedToTz();
    }
    return this.query.timeGroupedColumn(
      this.query.safeEvaluateSymbolContext().granularityOverride || this.granularity,
      this.convertedToTz()
    );
  }

  convertedToTz() {
    return this.query.convertTz(this.query.dimensionSql(this));
  }

  filterToWhere() {
    if (!this.dateRange) {
      return null;
    }
    return super.filterToWhere();
  }

  filterParams() {
    if (!this.dateRange) {
      return [];
    }
    return super.filterParams();
  }

  dateFromFormatted() {
    return this.formatFromDate(this.dateRange[0]);
  }

  dateFrom() {
    return this.inDbTimeZoneDateFrom(this.dateRange[0]);
  }

  dateFromParam() {
    return this.query.paramAllocator.allocateParamsForQuestionString(
      this.query.timeStampParam(this), [this.dateFrom()]
    );
  }

  dateToFormatted() {
    return this.formatToDate(this.dateRange[1]);
  }

  dateTo() {
    return this.inDbTimeZoneDateTo(this.dateRange[1]);
  }

  dateToParam() {
    return this.query.paramAllocator.allocateParamsForQuestionString(
      this.query.timeStampParam(this), [this.dateTo()]
    );
  }

  timeSeries() {
    if (!this.dateRange) {
      throw new UserError(`Time series queries without dateRange aren't supported`);
    }
    if (!this.granularity) {
      return [
        [this.dateFromFormatted(), this.dateToFormatted()]
      ];
    }
    const range = moment.range(this.dateFromFormatted(), this.dateToFormatted());
    if (!TIME_SERIES[this.granularity]) {
      throw new UserError(`Unsupported time granularity: ${this.granularity}`);
    }
    return TIME_SERIES[this.granularity](range);
  }
}

module.exports = BaseTimeDimension;