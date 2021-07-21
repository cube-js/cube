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
    .map(d => [d.startOf('isoWeek').format('YYYY-MM-DDT00:00:00.000'), d.endOf('isoWeek').format('YYYY-MM-DDT23:59:59.999')])
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

export const inDbTimeZone = (timezone: string, timestampFormat: string, timestamp: string): string => (
  moment.tz(timestamp, timezone).utc().format(timestampFormat)
);

export const extractDate = (data: any): string => {
  data = JSON.parse(JSON.stringify(data));
  return moment.tz(data[0] && data[0][Object.keys(data[0])[0]], 'UTC').utc().format(moment.HTML5_FMT.DATETIME_LOCAL_MS);
};

export const addSecondsToLocalTimestamp = (timestamp: string, timezone: string, seconds: number): Date => (
  moment.tz(timestamp, timezone)
    .add(seconds, 'second')
    .toDate()
);
