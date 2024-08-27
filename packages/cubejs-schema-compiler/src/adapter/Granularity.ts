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

  public readonly granularityOffset: string | undefined;

  public readonly origin: moment.Moment;

  private readonly predefinedGranularity: boolean;

  public constructor(
    private readonly query: BaseQuery,
    timeDimension: any
  ) {
    this.granularity = timeDimension.granularity;
    this.predefinedGranularity = isPredefinedGranularity(this.granularity);
    this.origin = moment().startOf('year'); // Defaults to current year start

    if (this.predefinedGranularity) {
      this.granularityInterval = `1 ${this.granularity}`;
    } else {
      const customGranularity = this.query.cacheValue(
        ['customGranularity', timeDimension.dimension, this.granularity],
        () => query.cubeEvaluator
          .byPath('dimensions', timeDimension.dimension)
          .granularities?.[this.granularity]
      );

      if (!customGranularity) {
        throw new Error(`Granularity "${timeDimension.granularity}" does not exist in dimension ${timeDimension.dimension}`);
      }

      this.granularityInterval = customGranularity.interval;

      if (customGranularity.origin) {
        this.origin = moment(new Date(customGranularity.origin));
      } else if (customGranularity.offset) {
        this.granularityOffset = customGranularity.offset;
        this.origin = addInterval(this.origin, parseSqlInterval(customGranularity.offset));
      }
    }
  }

  public originFormatted(): string {
    return this.origin.format('YYYY-MM-DDTHH:mm:ss.SSS');
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

    return this.granularityFromInterval();
  }

  public timeSeriesForInterval(dateRange: QueryDateRange, options: TimeSeriesOptions = { timestampPrecision: 3 }): QueryDateRange[] {
    if (this.predefinedGranularity) {
      return timeSeries(this.granularity, dateRange, options);
    }

    return timeSeriesFromCustomInterval(this.granularityInterval, dateRange, this.origin, options);
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
    if (interval.match(/second/)) {
      return 'second';
    } else if (interval.match(/minute/)) {
      return 'minute';
    } else if (interval.match(/hour/)) {
      return 'hour';
    } else if (interval.match(/day/)) {
      return 'day';
    } else if (interval.match(/week/)) {
      return 'week';
    } else if (interval.match(/month/)) {
      return 'month';
    } else if (interval.match(/quarter/)) {
      return 'quarter';
    } else /* if (interval.match(/year/)) */ {
      return 'year';
    }
  }

}
