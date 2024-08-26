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
        this.origin = moment(new Date(origin));
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
        this.query.granularityFromInterval(this.granularityInterval),
        this.query.granularityFromInterval(this.granularityOffset)
      );
    }

    return this.query.granularityFromInterval(this.granularityInterval);
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

    return this.query.granularityFromInterval(this.granularityInterval);
  }
}
