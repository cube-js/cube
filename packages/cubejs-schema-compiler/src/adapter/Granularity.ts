import moment from 'moment-timezone';
import {
  addInterval,
  isPredefinedGranularity, parseSqlInterval,
  QueryDateRange, timeSeries,
  timeSeriesFromCustomInterval,
  TimeSeriesOptions
} from '@cubejs-backend/shared';
import { BaseQuery } from './BaseQuery';

export class Granularity {
  public readonly granularity: string;

  public readonly granularityInterval: string;

  public readonly queryTimezone: string;

  public readonly granularityOffset: string | undefined;

  public readonly origin: moment.Moment;

  private readonly predefinedGranularity: boolean;

  public constructor(
    private readonly query: BaseQuery,
    timeDimension: any
  ) {
    this.granularity = timeDimension.granularity;
    this.predefinedGranularity = isPredefinedGranularity(this.granularity);
    this.queryTimezone = query.timezone;
    this.origin = moment.tz(query.timezone).startOf('year'); // Defaults to current year start

    if (this.predefinedGranularity) {
      this.granularityInterval = `1 ${this.granularity}`;
    } else {
      const customGranularity = this.query.cacheValue(
        ['customGranularity', timeDimension.dimension, this.granularity],
        () => query.cubeEvaluator
          .resolveGranularity([...query.cubeEvaluator.parsePath('dimensions', timeDimension.dimension), 'granularities', this.granularity])
      );

      if (!customGranularity) {
        throw new Error(`Granularity "${timeDimension.granularity}" does not exist in dimension ${timeDimension.dimension}`);
      }

      this.granularityInterval = customGranularity.interval;

      if (customGranularity.origin) {
        this.origin = moment.tz(customGranularity.origin, query.timezone);
      } else if (customGranularity.offset) {
        this.granularityOffset = customGranularity.offset;
        this.origin = addInterval(this.origin, parseSqlInterval(customGranularity.offset));
      }
    }
  }

  public isPredefined(): boolean {
    return this.predefinedGranularity;
  }

  /**
   * @returns origin date string in Query timezone
   */
  public originLocalFormatted(): string {
    return this.origin.tz(this.queryTimezone).format('YYYY-MM-DDTHH:mm:ss.SSS');
  }

  /**
   * @returns origin date string in UTC timezone
   */
  public originUtcFormatted(): string {
    return this.origin.clone().utc().format('YYYY-MM-DDTHH:mm:ss.SSSZ');
  }

  public minGranularity(): string {
    if (this.predefinedGranularity) {
      return this.granularity;
    }

    if (this.granularityOffset) {
      return this.query.minGranularity(
        this.granularityFromInterval(),
        this.granularityFromOffset()
      );
    }

    if (this.origin) {
      return this.query.minGranularity(
        this.granularityFromInterval(),
        this.query.granularityFor(this.origin.utc())
      );
    }

    return this.granularityFromInterval();
  }

  public timeSeriesForInterval(dateRange: QueryDateRange, options: TimeSeriesOptions = { timestampPrecision: 3 }): QueryDateRange[] {
    if (this.predefinedGranularity) {
      return timeSeries(this.granularity, dateRange, options);
    }

    // Interval range doesn't take timezone into account and operate in kinda local timezone,
    // but origin is treated as a timestamp in query timezone, so we pass it as the naive timestamp
    // to be in sync with date range during calculation.
    return timeSeriesFromCustomInterval(this.granularityInterval, dateRange, moment(this.originLocalFormatted()), options);
  }

  public resolvedGranularity(): string {
    if (this.predefinedGranularity) {
      return this.granularity;
    }

    return this.granularityFromInterval();
  }

  /**
   * Returns the smallest granularity for the granularityInterval
   */
  public granularityFromInterval(): string {
    return this.granularityFromIntervalString(this.granularityInterval);
  }

  /**
   * Returns the smallest granularity for the granularityOffset
   */
  public granularityFromOffset(): string {
    return this.granularityOffset ? this.granularityFromIntervalString(this.granularityOffset) : '';
  }

  /**
   * Returns the smallest granularity for the provided interval string
   * Interval may be presented as `1 year 2 months 3 weeks 4 days 5 hours 6 minutes 7 seconds
   * It is important to bubble up from the smallest, as this is used e.g. for minimum rollup granularity
   */
  private granularityFromIntervalString(interval: string): string {
    const intervalParsed = parseSqlInterval(interval);
    const intervalKeys = Object.keys(intervalParsed);

    if (intervalKeys.length === 1) {
      return intervalKeys[0];
    }

    if (intervalParsed.second) {
      return 'second';
    } else if (intervalParsed.minute) {
      return 'minute';
    } else if (intervalParsed.hour) {
      return 'hour';
    } else if (intervalParsed.day) {
      return 'day';
    } else if (intervalParsed.week) {
      return 'day';
    } else if (intervalParsed.month) {
      return 'month';
    } else if (intervalParsed.quarter) { // Only quarter+years possible
      return 'month';
    } else /* if (intervalParsed.year) */ {
      return 'year';
    }
  }

  public isNaturalAligned(): boolean {
    const intParsed = this.granularityInterval.split(' ');

    return !(intParsed.length !== 2 || intParsed[0] !== '1');
  }
}
