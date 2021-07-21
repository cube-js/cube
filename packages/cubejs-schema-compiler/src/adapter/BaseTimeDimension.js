import Moment from 'moment-timezone';
import { extendMoment } from 'moment-range';
import { timeSeries, FROM_PARTITION_RANGE, TO_PARTITION_RANGE } from '@cubejs-backend/shared';

import { BaseFilter } from './BaseFilter';
import { UserError } from '../compiler/UserError';

const moment = extendMoment(Moment);

export class BaseTimeDimension extends BaseFilter {
  constructor(query, timeDimension) {
    super(query, {
      dimension: timeDimension.dimension,
      operator: 'in_date_range',
      values: timeDimension.dateRange
    });
    this.query = query;
    this.dateRange = timeDimension.dateRange;
    this.granularity = timeDimension.granularity;
    this.boundaryDateRange = timeDimension.boundaryDateRange;
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

  unescapedAliasName(granularity) {
    const actualGranularity = granularity || this.granularity || 'day';

    return `${this.query.aliasName(this.dimension)}_${actualGranularity}`; // TODO date here for rollups
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
    const context = this.query.safeEvaluateSymbolContext();
    const granularity = context.granularityOverride || this.granularity;

    if (context.rollupQuery) {
      if (context.rollupGranularity === this.granularity) {
        return super.dimensionSql();
      }
      return this.query.timeGroupedColumn(granularity, this.query.dimensionSql(this));
    }
    if (context.ungrouped) {
      return this.convertedToTz();
    }
    return this.query.timeGroupedColumn(granularity, this.convertedToTz());
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
    if (!this.dateFromFormattedValue) {
      this.dateFromFormattedValue = this.formatFromDate(this.dateRange[0]);
    }

    return this.dateFromFormattedValue;
  }

  dateFrom() {
    if (!this.dateFromValue) {
      this.dateFromValue = this.inDbTimeZoneDateFrom(this.dateRange[0]);
    }

    return this.dateFromValue;
  }

  dateFromParam() {
    return this.query.paramAllocator.allocateParamsForQuestionString(
      this.query.timeStampParam(this), [this.dateFrom()]
    );
  }

  localDateTimeFromParam() {
    return this.query.dateTimeCast(this.query.paramAllocator.allocateParam(this.dateFromFormatted()));
  }

  dateToFormatted() {
    if (!this.dateToFormattedValue) {
      this.dateToFormattedValue = this.formatToDate(this.dateRange[1]);
    }
    return this.dateToFormattedValue;
  }

  dateTo() {
    if (!this.dateToValue) {
      this.dateToValue = this.inDbTimeZoneDateTo(this.dateRange[1]);
    }
    return this.dateToValue;
  }

  dateToParam() {
    return this.query.paramAllocator.allocateParamsForQuestionString(
      this.query.timeStampParam(this), [this.dateTo()]
    );
  }

  localDateTimeToParam() {
    return this.query.dateTimeCast(this.query.paramAllocator.allocateParam(this.dateToFormatted()));
  }

  dateRangeGranularity() {
    if (!this.dateRange) {
      return null;
    }
    const msFrom = moment.tz(this.dateFromFormatted(), this.query.timezone);
    const msTo = moment.tz(this.dateToFormatted(), this.query.timezone).add(1, 'ms');
    return this.query.minGranularity(
      this.query.granularityFor(msFrom),
      this.query.granularityFor(msTo),
    );
  }

  rollupGranularity() {
    if (!this.rollupGranularityValue) {
      this.rollupGranularityValue =
        this.query.cacheValue(
          ['rollupGranularity', this.granularity].concat(this.dateRange),
          () => this.query.minGranularity(this.granularity, this.dateRangeGranularity())
        );
    }
    return this.rollupGranularityValue;
  }

  timeSeries() {
    if (!this.dateRange) {
      throw new UserError('Time series queries without dateRange aren\'t supported');
    }

    if (!this.granularity) {
      return [
        [this.dateFromFormatted(), this.dateToFormatted()]
      ];
    }

    return timeSeries(this.granularity, [this.dateFromFormatted(), this.dateToFormatted()]);
  }

  wildcardRange() {
    return [FROM_PARTITION_RANGE, TO_PARTITION_RANGE];
  }

  boundaryDateRangeFormatted() {
    // TODO or here is due to boundaryDateRange can be defined in originalSql query used by rollup
    // TODO and dateRange can be defined in rollup query
    return this.boundaryDateRange && [
      this.formatFromDate(this.boundaryDateRange[0]),
      this.formatToDate(this.boundaryDateRange[1])
    ] || [this.dateFromFormatted(), this.dateToFormatted()];
  }
}
