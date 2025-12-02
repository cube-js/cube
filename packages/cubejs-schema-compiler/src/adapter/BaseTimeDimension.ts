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
  public readonly dateRange: [string, string];

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
    if (!this.granularityObj) {
      return null;
    }

    return super.selectColumns();
  }

  public hasNoRemapping() {
    if (!this.granularityObj) {
      return false;
    }

    return super.hasNoRemapping();
  }

  public aliasName() {
    if (!this.granularityObj) {
      return null;
    }

    return super.aliasName();
  }

  public unescapedAliasName(granularity?: string) {
    const actualGranularity = granularity || this.granularityObj?.granularity || 'day';

    const fullName = `${this.dimension}.${actualGranularity}`;
    if (this.query.options.memberToAlias?.[fullName]) {
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

    if (context.renderedReference?.[path]) {
      return context.renderedReference[path];
    }

    const dimDefinition = this.dimensionDefinition() as DimensionDefinition;
    const isLocalTime = dimDefinition.localTime;

    if (context.rollupQuery || context.wrapQuery) {
      if (context.rollupGranularity === this.granularityObj?.granularity) {
        return super.dimensionSql();
      }

      if (isLocalTime && granularity) {
        return this.localTimeGroupedColumn(this.query.dimensionSql(this), granularity);
      }
      return this.query.dimensionTimeGroupedColumn(this.query.dimensionSql(this), <Granularity>granularity);
    }

    if (context.ungrouped) {
      return this.convertedToTz();
    }

    // For localTime dimensions with granularity, use UTC timezone for grouping
    if (isLocalTime && granularity) {
      return this.localTimeGroupedColumn(this.convertedToTz(), granularity);
    }

    return this.query.dimensionTimeGroupedColumn(this.convertedToTz(), <Granularity>granularity);
  }

  /**
   * For localTime dimensions, apply time grouping without timezone conversion.
   * This uses UTC as the timezone to preserve the local time values.
   */
  private localTimeGroupedColumn(dimension: string, granularity: Granularity): string {
    // Temporarily override the query's timezone to UTC for grouping
    const originalTimezone = this.query.timezone;
    try {
      (this.query as any).timezone = 'UTC';
      return this.query.dimensionTimeGroupedColumn(dimension, granularity);
    } finally {
      (this.query as any).timezone = originalTimezone;
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
    const dimDefinition = this.dimensionDefinition() as DimensionDefinition;
    // Skip timezone conversion for local time dimensions
    if (dimDefinition.localTime) {
      return this.query.dimensionSql(this);
    }
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

  protected dateFromFormattedValue: string | null = null;

  public dateFromFormatted() {
    if (!this.dateFromFormattedValue) {
      const formatted = this.formatFromDate(this.dateRange[0]);
      const dimDefinition = this.dimensionDefinition() as DimensionDefinition;
      // For local time dimensions, remove any ISO 8601 timezone suffix
      // This includes: Z (UTC), +hh:mm, -hh:mm, +hhmm, -hhmm, +hh, -hh
      if (dimDefinition.localTime) {
        this.dateFromFormattedValue = this.stripTimezoneSuffix(formatted);
      } else {
        this.dateFromFormattedValue = formatted;
      }
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
    const dimDefinition = this.dimensionDefinition() as DimensionDefinition;
    // For local time dimensions, use local datetime params (no timezone conversion)
    if (dimDefinition.localTime) {
      return this.localDateTimeFromParam();
    }
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

  protected dateToFormattedValue: string | null = null;

  public dateToFormatted() {
    if (!this.dateToFormattedValue) {
      const formatted = this.formatToDate(this.dateRange[1]);
      const dimDefinition = this.dimensionDefinition() as DimensionDefinition;
      // For local time dimensions, remove any ISO 8601 timezone suffix
      // This includes: Z (UTC), +hh:mm, -hh:mm, +hhmm, -hhmm, +hh, -hh
      if (dimDefinition.localTime) {
        this.dateToFormattedValue = this.stripTimezoneSuffix(formatted);
      } else {
        this.dateToFormattedValue = formatted;
      }
    }

    return this.dateToFormattedValue;
  }

  /**
   * Strips ISO 8601 timezone designators from a datetime string.
   * Handles all valid ISO 8601 timezone formats:
   * - Z (UTC)
   * - ±hh:mm (e.g., +05:30, -08:00)
   * - ±hhmm (e.g., +0530, -0800)
   * - ±hh (e.g., +05, -08)
   * 
   * Only strips timezone info from timestamps (containing 'T'), not from date-only strings.
   */
  private stripTimezoneSuffix(dateString: string): string {
    if (!dateString) {
      return dateString;
    }
    
    // Only strip timezone if this is a timestamp (contains 'T'), not a date-only string
    if (dateString.includes('T')) {
      // Match ISO 8601 timezone designators at the end of the string:
      // Z | [+-]hh:mm | [+-]hhmm | [+-]hh
      return dateString.replace(/(?:Z|[+-]\d{2}(?::?\d{2})?)$/, '');
    }
    
    return dateString;
  }

  /**
   * Override formatFromDate for localTime dimensions to completely skip timezone conversion.
   * For localTime dimensions, we want to preserve the exact datetime value without any timezone shifts.
   */
  public formatFromDate(date: string): string {
    const dimDefinition = this.dimensionDefinition() as DimensionDefinition;
    if (dimDefinition.localTime && date) {
      // Strip timezone suffix from input and format without timezone conversion
      const strippedDate = this.stripTimezoneSuffix(date);
      
      // Format directly without timezone conversion
      return this.formatLocalDateTime(strippedDate, true);
    }
    return super.formatFromDate(date);
  }

  /**
   * Override formatToDate for localTime dimensions to completely skip timezone conversion.
   * For localTime dimensions, we want to preserve the exact datetime value without any timezone shifts.
   */
  public formatToDate(date: string): string {
    const dimDefinition = this.dimensionDefinition() as DimensionDefinition;
    if (dimDefinition.localTime && date) {
      // Strip timezone suffix from input and format without timezone conversion
      const strippedDate = this.stripTimezoneSuffix(date);
      // Format directly without timezone conversion
      return this.formatLocalDateTime(strippedDate, false);
    }
    return super.formatToDate(date);
  }

  /**
   * Override inDbTimeZoneDateFrom for localTime dimensions to skip timezone conversion.
   * For localTime dimensions, we want to use the formatted date directly without converting to DB timezone.
   */
  public inDbTimeZoneDateFrom(date: any): any {
    const dimDefinition = this.dimensionDefinition() as DimensionDefinition;
    if (dimDefinition.localTime) {
      // For localTime, return the formatted date without timezone conversion
      return this.formatFromDate(date);
    }
    return super.inDbTimeZoneDateFrom(date);
  }

  /**
   * Override inDbTimeZoneDateTo for localTime dimensions to skip timezone conversion.
   * For localTime dimensions, we want to use the formatted date directly without converting to DB timezone.
   */
  public inDbTimeZoneDateTo(date: any): any {
    const dimDefinition = this.dimensionDefinition() as DimensionDefinition;
    if (dimDefinition.localTime) {
      // For localTime, return the formatted date without timezone conversion
      return this.formatToDate(date);
    }
    return super.inDbTimeZoneDateTo(date);
  }

  /**
   * Format a datetime string for localTime dimensions without applying timezone conversion.
   * This ensures the datetime value stays exactly as specified, treating it as local time.
   */
  private formatLocalDateTime(date: string, isFromDate: boolean): string {
    if (!date) {
      return date;
    }

    const dateTimeLocalMsRegex = /^\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d\.\d\d\d$/;
    const dateTimeLocalURegex = /^\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d\.\d\d\d\d\d\d$/;
    const dateRegex = /^\d\d\d\d-\d\d-\d\d$/;

    const precision = this.query.timestampPrecision();
    
    // If already in correct format with correct precision, return as-is
    if (precision === 3 && date.match(dateTimeLocalMsRegex)) {
      return date;
    }
    if (precision === 6) {
      if (date.length === 23 && date.match(dateTimeLocalMsRegex)) {
        // Handle special case for formatToDate with .999
        if (!isFromDate && date.endsWith('.999')) {
          return `${date}999`;
        }
        return `${date}000`;
      }
      if (date.length === 26 && date.match(dateTimeLocalURegex)) {
        return date;
      }
    }

    // Handle date-only format (YYYY-MM-DD)
    if (date.match(dateRegex)) {
      const time = isFromDate ? '00:00:00' : '23:59:59';
      const fractional = isFromDate ? '0'.repeat(precision) : '9'.repeat(precision);
      return `${date}T${time}.${fractional}`;
    }

    // Parse the date WITHOUT timezone conversion using moment() instead of moment.tz()
    const m = moment(date);
    if (!m.isValid()) {
      return date;
    }

    // Format based on whether this is a from or to date
    if (isFromDate) {
      return m.format(`YYYY-MM-DDTHH:mm:ss.${'S'.repeat(precision)}`);
    } else {
      // For "to" dates, if time is exactly midnight, set to end of day
      if (m.format('HH:mm:ss') === '00:00:00') {
        return m.format(`YYYY-MM-DDT23:59:59.${'9'.repeat(precision)}`);
      }
      return m.format(`YYYY-MM-DDTHH:mm:ss.${'S'.repeat(precision)}`);
    }
  }

  protected dateToValue: any | null = null;

  public dateTo() {
    if (!this.dateToValue) {
      this.dateToValue = this.inDbTimeZoneDateTo(this.dateRange[1]);
    }
    return this.dateToValue;
  }

  public dateToParam() {
    const dimDefinition = this.dimensionDefinition() as DimensionDefinition;
    // For local time dimensions, use local datetime params (no timezone conversion)
    if (dimDefinition.localTime) {
      return this.localDateTimeToParam();
    }
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

  public resolvedGranularityAsIs() {
    return this.granularityObj ? this.granularityObj.granularity : null;
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
