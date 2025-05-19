import moment from 'moment-timezone';
import {
  FROM_PARTITION_RANGE,
  TO_PARTITION_RANGE,
  BUILD_RANGE_START_LOCAL,
  BUILD_RANGE_END_LOCAL
} from '@cubejs-backend/shared';

import { BaseFilter } from './BaseFilter';
import { UserError } from '../compiler/UserError';
import { BaseQuery } from './BaseQuery';
import { DimensionDefinition, SegmentDefinition } from '../compiler/CubeEvaluator';
import { Granularity } from './Granularity';

export class BaseTimeDimension extends BaseFilter {
  public readonly dateRange: any;

  public readonly granularityObj: Granularity | undefined;

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
    if (timeDimension.granularity) {
      this.granularityObj = new Granularity(query, timeDimension);
    }
    this.boundaryDateRange = timeDimension.boundaryDateRange;
    this.shiftInterval = timeDimension.shiftInterval;
  }

  // TODO: find and fix all hidden references to granularity to rely on granularityObj instead?
  public get granularity(): string | null | undefined {
    return this.granularityObj?.granularity;
  }

  public selectColumns() {
    const context = this.query.safeEvaluateSymbolContext();
    if (!context.granularityOverride && !this.granularityObj) {
      return null;
    }

    return super.selectColumns();
  }

  public hasNoRemapping() {
    const context = this.query.safeEvaluateSymbolContext();
    if (!context.granularityOverride && !this.granularityObj) {
      return false;
    }

    return super.hasNoRemapping();
  }

  public aliasName() {
    const context = this.query.safeEvaluateSymbolContext();
    if (!context.granularityOverride && !this.granularityObj) {
      return null;
    }

    return super.aliasName();
  }

  public unescapedAliasName(granularity?: string) {
    const actualGranularity = granularity || this.granularityObj?.granularity || 'day';

    const fullName = `${this.dimension}.${actualGranularity}`;
    if (this.query.options.memberToAlias && this.query.options.memberToAlias[fullName]) {
      return this.query.options.memberToAlias[fullName];
    }

    return `${this.query.aliasName(this.dimension)}_${actualGranularity}`; // TODO date here for rollups
  }

  public dateSeriesAliasName() {
    return this.query.escapeColumnName(`${this.dimension}_series`);
  }

  public dateSeriesSelectColumn(dateSeriesAliasName: string | null, dateSeriesGranularity?: string) {
    if (!this.granularityObj) {
      return null;
    }

    // In case of query with more than one granularity, the time series table was generated
    // with the minimal granularity among all. If this is our granularity, we can save
    // some cpu cycles without 'date_from' truncation. But if this is not our granularity,
    // we need to truncate it to desired.
    if (dateSeriesGranularity && this.granularityObj?.granularity !== dateSeriesGranularity) {
      return `${this.query.dimensionTimeGroupedColumn(`${dateSeriesAliasName || this.dateSeriesAliasName()}.${this.query.escapeColumnName('date_from')}`, this.granularityObj)} ${this.aliasName()}`;
    }
    return `${dateSeriesAliasName || this.dateSeriesAliasName()}.${this.query.escapeColumnName('date_from')} ${this.aliasName()}`;
  }

  public dimensionSql() {
    const context = this.query.safeEvaluateSymbolContext();
    const granularityName = context.granularityOverride || this.granularityObj?.granularity;
    const path = granularityName ? `${this.expressionPath()}.${granularityName}` : this.expressionPath();
    const granularity = granularityName && this.granularityObj?.granularity !== granularityName ?
      new Granularity(this.query, {
        dimension: this.dimension,
        granularity: granularityName
      }) : this.granularityObj;

    if ((context.renderedReference || {})[path]) {
      return context.renderedReference[path];
    }

    if (context.rollupQuery || context.wrapQuery) {
      if (context.rollupGranularity === this.granularityObj?.granularity) {
        return super.dimensionSql();
      }

      return this.query.dimensionTimeGroupedColumn(this.query.dimensionSql(this), <Granularity>granularity);
    }

    if (context.ungrouped) {
      return this.convertedToTz();
    }

    return this.query.dimensionTimeGroupedColumn(this.convertedToTz(), <Granularity>granularity);
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

  public dateRangeGranularity(): string | null {
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

  protected rollupGranularityValue: string | null = null;

  public rollupGranularity(): string | null {
    if (!this.rollupGranularityValue) {
      this.rollupGranularityValue =
        this.query.cacheValue(
          ['rollupGranularity', this.granularityObj?.granularity].concat(this.dateRange),
          () => {
            if (!this.granularityObj) {
              return this.dateRangeGranularity();
            }

            // If we have granularity and date range, we need to check
            // that the interval and the granularity offset are stacked/fits with date range
            if (this.dateRange && (this.granularityObj.isPredefined() ||
              !this.granularityObj.isAlignedWithDateRange([this.dateFromFormatted(), this.dateToFormatted()]))) {
              return this.query.minGranularity(this.granularityObj.minGranularity(), this.dateRangeGranularity());
            }

            // We return the granularity as-is, including custom ones,
            // because baseQuery.granularityHierarchies correctly expands all custom granularities into hierarchies.
            return this.granularityObj.granularity;
          }
        );
    }

    return this.rollupGranularityValue;
  }

  public timeSeries() {
    if (!this.dateRange) {
      throw new UserError('Time series queries without dateRange aren\'t supported');
    }

    if (!this.granularityObj) {
      return [[this.dateFromFormatted(), this.dateToFormatted()]];
    }

    return this.granularityObj.timeSeriesForInterval([this.dateFromFormatted(), this.dateToFormatted()], { timestampPrecision: this.query.timestampPrecision() });
  }

  public resolvedGranularity() {
    return this.granularityObj ? this.granularityObj.resolvedGranularity() : null;
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
