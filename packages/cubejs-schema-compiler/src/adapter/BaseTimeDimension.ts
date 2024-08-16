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

  public readonly baseRollupGranularity: string | undefined;

  public readonly leadingWindowPart: string | undefined;

  public readonly trailingWindowPart: string | undefined;

  public readonly binStride: { num: number, type: string } | undefined;

  public readonly granularitySql: Function | undefined;

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
    if (!this.isPredefined) {
      const customGranularity = this.query.cubeEvaluator
        .byPath('dimensions', timeDimension.dimension)
        .granularities?.[this.granularity];

      this.baseRollupGranularity = customGranularity?.baseGranularity;
      this.granularitySql = customGranularity?.sql;
      this.leadingWindowPart = customGranularity?.leading;
      this.trailingWindowPart = customGranularity?.trailing;
      if (customGranularity?.bin) {
        // XXX: Potentially to use BaseQuery#parseInterval?
        // But it's protected and expects duration to be specified, so no "bin: year" will be possible
        const v = customGranularity.bin.trim().split(' ');
        if (v.length === 1) {
          this.binStride = { num: 1, type: v[0] };
        } else {
          this.binStride = { num: parseInt(v[0], 10), type: v[1] };
        }
      }
    }
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
      if (this.isPredefined || !this.granularity) {
        return this.query.timeGroupedColumn(granularity, this.query.dimensionSql(this));
      } else {
        return this.dimensionGranularitySql();
      }
    }

    if (context.ungrouped) {
      return this.convertedToTz();
    }

    if (this.isPredefined) {
      return this.query.timeGroupedColumn(granularity, this.convertedToTz());
    } else {
      return this.granularityConvertedToTz();
    }
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
    return this.query.convertTz(`${this.query.dimensionSql(this)}`);
  }

  public granularityConvertedToTz() {
    return this.query.convertTz(`(${this.dimensionGranularitySql()})`);
  }

  public dimensionGranularitySql() {
    if (this.granularitySql) { // Need to evaluate symbol's SQL
      return this.query.dimensionGranularitySql(this);
    }

    let dtDate = this.query.dimensionSql(this);

    // Need to construct SQL
    if (this.binStride?.num === 1) { // range is aligned with natural calendar, so we can use DATE_TRUNC
      if (this.leadingWindowPart) {
        // Example: DATE_TRUNC('granularity', dimension - INTERVAL 'xxxx') + INTERVAL 'xxxx'
        dtDate = this.query.subtractInterval(dtDate, this.leadingWindowPart);
        dtDate = this.query.timeGroupedColumn(this.granularityFromInterval(this.binStride.type), dtDate);
        dtDate = this.query.addInterval(dtDate, this.leadingWindowPart);

        return dtDate;
      } else if (this.trailingWindowPart) {
        // Example: DATE_TRUNC('granularity', dimension + INTERVAL 'xxxx') - INTERVAL 'xxxx'
        dtDate = this.query.addInterval(dtDate, this.trailingWindowPart);
        dtDate = this.query.timeGroupedColumn(this.granularityFromInterval(this.binStride.type), dtDate);
        dtDate = this.query.subtractInterval(dtDate, this.trailingWindowPart);

        return dtDate;
      }

      // No window offsets
      return this.query.timeGroupedColumn(this.granularityFromInterval(this.binStride.type), dtDate);
    }

    // need to use DATE_BIN
    let origin = this.query.startOfTheYearTimestampSql();
    if (this.leadingWindowPart) {
      origin = this.query.addInterval(origin, this.leadingWindowPart);
    } else if (this.trailingWindowPart) {
      origin = this.query.subtractInterval(origin, this.trailingWindowPart);
    }

    return this.query.dateBin(`${this.binStride?.num} ${this.binStride?.type}`, dtDate, origin);
  }

  public granularityFromInterval(interval: string) {
    if (!interval) {
      return undefined;
    }
    if (interval.match(/day/)) {
      return 'day';
    } else if (interval.match(/month/)) {
      return 'month';
    } else if (interval.match(/year/)) {
      return 'year';
    } else if (interval.match(/week/)) {
      return 'week';
    } else if (interval.match(/hour/)) {
      return 'hour';
    } else if (interval.match(/minute/)) {
      return 'minute';
    } else if (interval.match(/second/)) {
      return 'second';
    }
    return undefined;
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

            if (this.baseRollupGranularity) {
              return this.query.minGranularity(this.baseRollupGranularity, this.dateRangeGranularity());
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
