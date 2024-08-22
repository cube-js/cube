import { DateRange, extendMoment } from 'moment-range';
import { unitOfTime } from 'moment-timezone';

const Moment = require('moment-timezone');

const moment = extendMoment(Moment);

type QueryDateRange = [string, string];
type SqlInterval = string;
type TimeSeriesOptions = {
  timestampPrecision: number
};
type ParsedInterval = Partial<Record<unitOfTime.DurationConstructor, number>>;

export const TIME_SERIES: Record<string, (range: DateRange, timestampPrecision: number) => QueryDateRange[]> = {
  day: (range: DateRange, digits) => Array.from(range.snapTo('day').by('day'))
    .map(d => [d.format(`YYYY-MM-DDT00:00:00.${'0'.repeat(digits)}`), d.format(`YYYY-MM-DDT23:59:59.${'9'.repeat(digits)}`)]),
  month: (range: DateRange, digits) => Array.from(range.snapTo('month').by('month'))
    .map(d => [d.format(`YYYY-MM-01T00:00:00.${'0'.repeat(digits)}`), d.endOf('month').format(`YYYY-MM-DDT23:59:59.${'9'.repeat(digits)}`)]),
  year: (range: DateRange, digits) => Array.from(range.snapTo('year').by('year'))
    .map(d => [d.format(`YYYY-01-01T00:00:00.${'0'.repeat(digits)}`), d.endOf('year').format(`YYYY-MM-DDT23:59:59.${'9'.repeat(digits)}`)]),
  hour: (range: DateRange, digits) => Array.from(range.snapTo('hour').by('hour'))
    .map(d => [d.format(`YYYY-MM-DDTHH:00:00.${'0'.repeat(digits)}`), d.format(`YYYY-MM-DDTHH:59:59.${'9'.repeat(digits)}`)]),
  minute: (range: DateRange, digits) => Array.from(range.snapTo('minute').by('minute'))
    .map(d => [d.format(`YYYY-MM-DDTHH:mm:00.${'0'.repeat(digits)}`), d.format(`YYYY-MM-DDTHH:mm:59.${'9'.repeat(digits)}`)]),
  second: (range: DateRange, digits) => Array.from(range.snapTo('second').by('second'))
    .map(d => [d.format(`YYYY-MM-DDTHH:mm:ss.${'0'.repeat(digits)}`), d.format(`YYYY-MM-DDTHH:mm:ss.${'9'.repeat(digits)}`)]),
  week: (range: DateRange, digits) => Array.from(range.snapTo(<unitOfTime.Diff>'isoWeek').by('week'))
    .map(d => [d.startOf('isoWeek').format(`YYYY-MM-DDT00:00:00.${'0'.repeat(digits)}`), d.endOf('isoWeek').format(`YYYY-MM-DDT23:59:59.${'9'.repeat(digits)}`)]),
  quarter: (range: DateRange, digits) => Array.from(range.snapTo('quarter').by('quarter'))
    .map(d => [d.format(`YYYY-MM-DDT00:00:00.${'0'.repeat(digits)}`), d.endOf('quarter').format(`YYYY-MM-DDT23:59:59.${'9'.repeat(digits)}`)]),
};

/**
 * Parse PostgreSQL-like interval string into object
 * E.g. '2 years 15 months 100 weeks 99 hours 15 seconds'
 * Negative units are also supported
 * E.g. '-2 months 5 days -10 hours'
 */
function parseSqlInterval(intervalStr: SqlInterval): ParsedInterval {
  const regex = /(-?\d+)\s+(year|quarter|month|week|day|hour|minute|second)s?/g;
  const interval: ParsedInterval = {};

  for (const match of intervalStr.matchAll(regex)) {
    const value = parseInt(match[1], 10);
    const unit = match[2] as unitOfTime.DurationConstructor;
    interval[unit] = value;
  }

  return interval;
}

function addInterval(date: moment.Moment, interval: ParsedInterval): moment.Moment {
  const res = date.clone();

  Object.entries(interval).forEach(([key, value]) => {
    res.add(value, key as unitOfTime.DurationConstructor);
  });

  return res;
}

function subtractInterval(date: moment.Moment, interval: ParsedInterval): moment.Moment {
  const res = date.clone();

  Object.entries(interval).forEach(([key, value]) => {
    res.subtract(value, key as unitOfTime.DurationConstructor);
  });

  return res;
}

/**
 * Returns the closest date prior to date parameter aligned with the offset and interval
 * If no offset provided, the beginning of the year will be taken as pivot point
 */
function alignToOffset(date: moment.Moment, interval: ParsedInterval, offset?: ParsedInterval): moment.Moment {
  let alignedDate = date.clone();
  let intervalOp;

  const startOfYear = moment().year(date.year()).startOf('year');
  let offsetDate = offset ? addInterval(startOfYear, offset) : startOfYear;

  if (date.isBefore(offsetDate)) {
    intervalOp = offsetDate.isBefore(startOfYear) ? addInterval : subtractInterval;

    while (date.isBefore(offsetDate)) {
      offsetDate = intervalOp(offsetDate, interval);
    }
    alignedDate = offsetDate;
  } else if (offsetDate.isBefore(startOfYear)) {
    intervalOp = offsetDate.isBefore(startOfYear) ? addInterval : subtractInterval;

    while (date.isAfter(offsetDate)) {
      alignedDate = offsetDate.clone();
      offsetDate = intervalOp(offsetDate, interval);
    }
  } else {
    intervalOp = offsetDate.isBefore(startOfYear) ? subtractInterval : addInterval;

    while (date.isAfter(offsetDate)) {
      alignedDate = offsetDate.clone();
      offsetDate = intervalOp(offsetDate, interval);
    }
  }

  return alignedDate;
}

export const timeSeriesFromCustomInterval = (intervalStr: string, [startStr, endStr]: QueryDateRange, offsetStr?: string, options: TimeSeriesOptions = { timestampPrecision: 3 }): QueryDateRange[] => {
  const intervalParsed = parseSqlInterval(intervalStr);
  const offsetParsed = offsetStr ? parseSqlInterval(offsetStr) : undefined;
  const start = moment(startStr);
  const end = moment(endStr);
  let alignedStart = alignToOffset(start, intervalParsed, offsetParsed);

  const dates: QueryDateRange[] = [];

  while (alignedStart.isBefore(end)) {
    const s = alignedStart.clone();
    alignedStart = addInterval(alignedStart, intervalParsed);
    dates.push([
      s.format(`YYYY-MM-DDTHH:mm:ss.${'0'.repeat(options.timestampPrecision)}`),
      alignedStart.clone()
        .subtract(1, 'second')
        .format(`YYYY-MM-DDTHH:mm:ss.${'9'.repeat(options.timestampPrecision)}`)
    ]);
  }

  return dates;
};

export const timeSeries = (granularity: string, dateRange: QueryDateRange, options: TimeSeriesOptions = { timestampPrecision: 3 }): QueryDateRange[] => {
  if (!TIME_SERIES[granularity]) {
    // TODO error
    throw new Error(`Unsupported time granularity: ${granularity}`);
  }

  if (!options.timestampPrecision) {
    throw new Error(`options.timestampPrecision is required, actual: ${options.timestampPrecision}`);
  }

  // moment.range works with strings
  const range = moment.range(<any>dateRange[0], <any>dateRange[1]);

  return TIME_SERIES[granularity](range, options.timestampPrecision);
};

export const isPredefinedGranularity = (granularity: string): boolean => !!TIME_SERIES[granularity];

export const FROM_PARTITION_RANGE = '__FROM_PARTITION_RANGE';

export const TO_PARTITION_RANGE = '__TO_PARTITION_RANGE';

export const BUILD_RANGE_START_LOCAL = '__BUILD_RANGE_START_LOCAL';

export const BUILD_RANGE_END_LOCAL = '__BUILD_RANGE_END_LOCAL';

export const inDbTimeZone = (timezone: string, timestampFormat: string, timestamp: string): string => {
  if (timestamp.length === 23 || timestamp.length === 26) {
    const zone = moment.tz.zone(timezone);
    if (!zone) {
      throw new Error(`Unknown timezone: ${timezone}`);
    }

    const parsedTime = Date.parse(`${timestamp}Z`);
    const offset = zone.utcOffset(parsedTime);
    const inDbTimeZoneDate = new Date(parsedTime + offset * 60 * 1000);

    if (timestampFormat === 'YYYY-MM-DD[T]HH:mm:ss.SSS[Z]' || timestampFormat === 'YYYY-MM-DDTHH:mm:ss.SSSZ') {
      return inDbTimeZoneDate.toJSON();
    } else if (timestampFormat === 'YYYY-MM-DD[T]HH:mm:ss.SSSSSS[Z]' || timestampFormat === 'YYYY-MM-DDTHH:mm:ss.SSSSSSZ') {
      const value = inDbTimeZoneDate.toJSON();
      if (value.endsWith('999Z')) {
        // emulate microseconds
        return value.replace('Z', '999Z');
      }

      // emulate microseconds
      return value.replace('Z', '000Z');
    } else if (timestampFormat === 'YYYY-MM-DDTHH:mm:ss.SSS') {
      return inDbTimeZoneDate.toJSON().replace('Z', '');
    } else if (timestampFormat === 'YYYY-MM-DDTHH:mm:ss.SSSSSS') {
      // emulate microseconds
      return inDbTimeZoneDate.toJSON().replace('Z', '000');
    }
  }

  // moment doesn't support microseconds,
  // it will fill it with zeros
  return moment.tz(timestamp, timezone).utc().format(timestampFormat);
};

export const utcToLocalTimeZone = (timezone: string, timestampFormat: string, timestamp: string): string => {
  if (timestamp.length === 23) {
    const zone = moment.tz.zone(timezone);
    if (!zone) {
      throw new Error(`Unknown timezone: ${timezone}`);
    }
    const parsedTime = Date.parse(`${timestamp}Z`);
    // TODO parsedTime might be incorrect offset for conversion
    const offset = zone.utcOffset(parsedTime);
    const inDbTimeZoneDate = new Date(parsedTime - offset * 60 * 1000);
    if (timestampFormat === 'YYYY-MM-DD[T]HH:mm:ss.SSS[Z]' || timestampFormat === 'YYYY-MM-DDTHH:mm:ss.SSSZ') {
      return inDbTimeZoneDate.toJSON();
    } else if (timestampFormat === 'YYYY-MM-DDTHH:mm:ss.SSS') {
      return inDbTimeZoneDate.toJSON().replace('Z', '');
    }
  }

  return moment.tz(timestamp, 'UTC').tz(timezone).format(timestampFormat);
};

export const extractDate = (data: any): string | null => {
  if (!data) {
    return null;
  }
  data = JSON.parse(JSON.stringify(data));
  const value = data[0] && data[0][Object.keys(data[0])[0]];
  if (!value) {
    return value;
  }
  return moment.tz(value, 'UTC').utc().format(moment.HTML5_FMT.DATETIME_LOCAL_MS);
};

export const addSecondsToLocalTimestamp = (timestamp: string, timezone: string, seconds: number): Date => {
  if (timestamp.length === 23) {
    const zone = moment.tz.zone(timezone);
    if (!zone) {
      throw new Error(`Unknown timezone: ${timezone}`);
    }

    const parsedTime = Date.parse(`${timestamp}Z`);
    const offset = zone.utcOffset(parsedTime);
    return new Date(parsedTime + offset * 60 * 1000 + seconds * 1000);
  }

  return moment.tz(timestamp, timezone)
    .add(seconds, 'second')
    .toDate();
};

export const reformatInIsoLocal = (timestamp: string): string => {
  if (!timestamp) {
    return timestamp;
  }
  if (timestamp.length === 23) {
    return timestamp;
  }
  if (timestamp.length === 24) {
    return timestamp.replace('Z', '');
  }
  return moment.tz(timestamp, 'UTC').utc().format(moment.HTML5_FMT.DATETIME_LOCAL_MS);
};
