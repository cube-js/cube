import moment from 'moment-timezone';
import { timeSeries, isPredefinedGranularity, FROM_PARTITION_RANGE, TO_PARTITION_RANGE, BUILD_RANGE_START_LOCAL, BUILD_RANGE_END_LOCAL } from '@cubejs-backend/shared';

import { BaseFilter } from './BaseFilter';
import { UserError } from '../compiler/UserError';
import { BaseQuery } from './BaseQuery';
import { DimensionDefinition, SegmentDefinition } from '../compiler/CubeEvaluator';

export class BaseTimeDimension extends BaseFilter {
  public readonly dateRange: any;

  public readonly granularity: string;

  public readonly isPredefined: boolean;

  public readonly baseGranularity: string;

  public readonly boundaryDateRange: any;

  public readonly shiftInterval: string;

  public constructor(
    query: BaseQuery,
    timeDimension: any
  ) {
    super(query, {
      dimension: timeDimension.dimension,
      operator: 'in_date_range',
      values: timeDimension.dateRange
    });
    this.dateRange = timeDimension.dateRange;
    this.granularity = timeDimension.granularity;
    this.isPredefined = isPredefinedGranularity(this.granularity);
    this.baseGranularity = this.query.cubeEvaluator
      .byPath('dimensions', timeDimension.dimension)
      .granularities?.[this.granularity]?.baseGranularity;
    this.boundaryDateRange = timeDimension.boundaryDateRange;
    this.shiftInterval = timeDimension.shiftInterval;
  }

  public selectColumns() {
    const context = this.query.safeEvaluateSymbolContext();
    if (!context.granularityOverride && !this.granularity) {
      return null;
    }

    return super.selectColumns();
  }

  public hasNoRemapping() {
    const context = this.query.safeEvaluateSymbolContext();
    if (!context.granularityOverride && !this.granularity) {
      return false;
    }

    return super.hasNoRemapping();
  }

  public aliasName() {
    const context = this.query.safeEvaluateSymbolContext();
    if (!context.granularityOverride && !this.granularity) {
      return null;
    }

    return super.aliasName();
  }

  // @ts-ignore
  public unescapedAliasName(granularity: string) {
    const actualGranularity = granularity || this.granularity || 'day';

    return `${this.query.aliasName(this.dimension)}_${actualGranularity}`; // TODO date here for rollups
  }

  public dateSeriesAliasName() {
    return this.query.escapeColumnName(`${this.dimension}_series`);
  }

  public dateSeriesSelectColumn(dateSeriesAliasName) {
    if (!this.granularity) {
      return null;
    }
    return `${dateSeriesAliasName || this.dateSeriesAliasName()}.${this.query.escapeColumnName('date_from')} ${this.aliasName()}`;
  }

  public dimensionSql() {
    const context = this.query.safeEvaluateSymbolContext();
    const granularity = context.granularityOverride || this.granularity;
    const path = granularity ? `${this.expressionPath()}.${granularity}` : this.expressionPath();
    if ((context.renderedReference || {})[path]) {
      return context.renderedReference[path];
    }

    if (context.rollupQuery || context.wrapQuery) {
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

  public dimensionDefinition(): DimensionDefinition | SegmentDefinition {
    if (this.shiftInterval) {
      return { ...super.dimensionDefinition(), shiftInterval: this.shiftInterval };
    }
    return super.dimensionDefinition();
  }

  public convertTzForRawTimeDimensionIfNeeded(sql) {
    return sql();
  }

  public convertedToTz() {
    return this.query.convertTz(this.query.dimensionSql(this));
  }

  public filterToWhere() {
    if (!this.dateRange) {
      return null;
    }
    return super.filterToWhere();
  }

  public filterParams() {
    if (!this.dateRange) {
      return [];
    }
    return super.filterParams();
  }

  protected dateFromFormattedValue: any | null = null;

  public dateFromFormatted() {
    if (!this.dateFromFormattedValue) {
      this.dateFromFormattedValue = this.formatFromDate(this.dateRange[0]);
    }

    return this.dateFromFormattedValue;
  }

  protected dateFromValue: any | null = null;

  public dateFrom() {
    if (!this.dateFromValue) {
      this.dateFromValue = this.inDbTimeZoneDateFrom(this.dateRange[0]);
    }

    return this.dateFromValue;
  }

  public dateFromParam() {
    return this.query.paramAllocator.allocateParamsForQuestionString(
      this.query.timeStampParam(this), [this.dateFrom()]
    );
  }

  public localDateTimeFromParam() {
    return this.query.dateTimeCast(this.query.paramAllocator.allocateParam(this.dateFromFormatted()));
  }

  public localDateTimeFromOrBuildRangeParam() {
    return this.query.dateTimeCast(this.query.paramAllocator.allocateParam(this.dateRange ? this.dateFromFormatted() : BUILD_RANGE_START_LOCAL));
  }

  protected dateToFormattedValue: any | null = null;

  public dateToFormatted() {
    if (!this.dateToFormattedValue) {
      this.dateToFormattedValue = this.formatToDate(this.dateRange[1]);
    }

    return this.dateToFormattedValue;
  }

  protected dateToValue: any | null = null;

  public dateTo() {
    if (!this.dateToValue) {
      this.dateToValue = this.inDbTimeZoneDateTo(this.dateRange[1]);
    }
    return this.dateToValue;
  }

  public dateToParam() {
    return this.query.paramAllocator.allocateParamsForQuestionString(
      this.query.timeStampParam(this), [this.dateTo()]
    );
  }

  public localDateTimeToParam() {
    return this.query.dateTimeCast(this.query.paramAllocator.allocateParam(this.dateToFormatted()));
  }

  public localDateTimeToOrBuildRangeParam() {
    return this.query.dateTimeCast(this.query.paramAllocator.allocateParam(this.dateRange ? this.dateToFormatted() : BUILD_RANGE_END_LOCAL));
  }

  public dateRangeGranularity() {
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

  protected rollupGranularityValue: any | null = null;

  public rollupGranularity() {
    if (!this.rollupGranularityValue) {
      this.rollupGranularityValue =
        this.query.cacheValue(
          ['rollupGranularity', this.granularity].concat(this.dateRange),
          () => {
            if (this.isPredefined) {
              return this.query.minGranularity(this.granularity, this.dateRangeGranularity());
            }

            if (this.baseGranularity) {
              return this.query.minGranularity(this.baseGranularity, this.dateRangeGranularity());
            }

            // Trying to get granularity from the date range if it was provided
            return this.dateRangeGranularity();
          }
        );
    }

    return this.rollupGranularityValue;
  }

  public timeSeries() {
    if (!this.dateRange) {
      throw new UserError('Time series queries without dateRange aren\'t supported');
    }

    if (!this.granularity) {
      return [
        [this.dateFromFormatted(), this.dateToFormatted()]
      ];
    }

    return timeSeries(this.granularity, [this.dateFromFormatted(), this.dateToFormatted()], {
      timestampPrecision: this.query.timestampPrecision(),
    });
  }

  public wildcardRange() {
    return [FROM_PARTITION_RANGE, TO_PARTITION_RANGE];
  }

  public boundaryDateRangeFormatted() {
    // TODO or here is due to boundaryDateRange can be defined in originalSql query used by rollup
    // TODO and dateRange can be defined in rollup query
    return this.boundaryDateRange && [
      this.formatFromDate(this.boundaryDateRange[0]),
      this.formatToDate(this.boundaryDateRange[1])
    ] || [this.dateFromFormatted(), this.dateToFormatted()];
  }
}
