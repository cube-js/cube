import { DateRange, extendMoment } from 'moment-range';
import { unitOfTime } from 'moment-timezone';

const Moment = require('moment-timezone');

const moment = extendMoment(Moment);

type QueryDateRange = [string, string];

export const TIME_SERIES: { [key: string]: (range: DateRange) => QueryDateRange[] } = {
  day: (range: DateRange) => Array.from(range.snapTo('day').by('day'))
    .map(d => [d.format('YYYY-MM-DDT00:00:00.000'), d.format('YYYY-MM-DDT23:59:59.999')]),
  month: (range: DateRange) => Array.from(range.snapTo('month').by('month'))
    .map(d => [d.format('YYYY-MM-01T00:00:00.000'), d.endOf('month').format('YYYY-MM-DDT23:59:59.999')]),
  year: (range: DateRange) => Array.from(range.snapTo('year').by('year'))
    .map(d => [d.format('YYYY-01-01T00:00:00.000'), d.endOf('year').format('YYYY-MM-DDT23:59:59.999')]),
  hour: (range: DateRange) => Array.from(range.snapTo('hour').by('hour'))
    .map(d => [d.format('YYYY-MM-DDTHH:00:00.000'), d.format('YYYY-MM-DDTHH:59:59.999')]),
  minute: (range: DateRange) => Array.from(range.snapTo('minute').by('minute'))
    .map(d => [d.format('YYYY-MM-DDTHH:mm:00.000'), d.format('YYYY-MM-DDTHH:mm:59.999')]),
  second: (range: DateRange) => Array.from(range.snapTo('second').by('second'))
    .map(d => [d.format('YYYY-MM-DDTHH:mm:ss.000'), d.format('YYYY-MM-DDTHH:mm:ss.999')]),
  week: (range: DateRange) => Array.from(range.snapTo(<unitOfTime.Diff>'isoWeek').by('week'))
    .map(d => [d.startOf('isoWeek').format('YYYY-MM-DDT00:00:00.000'), d.endOf('isoWeek').format('YYYY-MM-DDT23:59:59.999')]),
  quarter: (range: DateRange) => Array.from(range.snapTo('quarter').by('quarter'))
    .map(d => [d.format('YYYY-MM-DDT00:00:00.000'), d.endOf('quarter').format('YYYY-MM-DDT23:59:59.999')]),
};

export const timeSeries = (granularity: string, dateRange: QueryDateRange): QueryDateRange[] => {
  if (!TIME_SERIES[granularity]) {
    // TODO error
    throw new Error(`Unsupported time granularity: ${granularity}`);
  }

  // moment.range works with strings
  const range = moment.range(<any>dateRange[0], <any>dateRange[1]);

  return TIME_SERIES[granularity](range);
};

export const FROM_PARTITION_RANGE = '__FROM_PARTITION_RANGE';

export const TO_PARTITION_RANGE = '__TO_PARTITION_RANGE';

export const BUILD_RANGE_START_LOCAL = '__BUILD_RANGE_START_LOCAL';

export const BUILD_RANGE_END_LOCAL = '__BUILD_RANGE_END_LOCAL';

export const inDbTimeZone = (timezone: string, timestampFormat: string, timestamp: string): string => {
  if (timestamp.length === 23) {
    const zone = moment.tz.zone(timezone);
    if (!zone) {
      throw new Error(`Unknown timezone: ${timezone}`);
    }
    const parsedTime = Date.parse(`${timestamp}Z`);
    const offset = zone.utcOffset(parsedTime);
    const inDbTimeZoneDate = new Date(parsedTime + offset * 60 * 1000);
    if (timestampFormat === 'YYYY-MM-DD[T]HH:mm:ss.SSS[Z]' || timestampFormat === 'YYYY-MM-DDTHH:mm:ss.SSSZ') {
      return inDbTimeZoneDate.toJSON();
    } else if (timestampFormat === 'YYYY-MM-DDTHH:mm:ss.SSS') {
      return inDbTimeZoneDate.toJSON().replace('Z', '');
    }
  }
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

export const extractDate = (data: any): string => {
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
