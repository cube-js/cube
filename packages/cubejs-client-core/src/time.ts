import dayjs from 'dayjs';
import quarterOfYear from 'dayjs/plugin/quarterOfYear';
import duration from 'dayjs/plugin/duration';
import isoWeek from 'dayjs/plugin/isoWeek';
import en from 'dayjs/locale/en';

dayjs.extend(quarterOfYear);
dayjs.extend(duration);
dayjs.extend(isoWeek);

export type SqlInterval = string;

// TODO: Define a better type as unitOfTime.DurationConstructor in moment.js
export type ParsedInterval = Record<string, number>;

export type Granularity = {
  interval: SqlInterval;
  origin?: string;
  offset?: SqlInterval;
};

export type DayRange = {
  by: (value: any) => dayjs.Dayjs[];
  snapTo: (value: any) => DayRange;
  start: dayjs.Dayjs;
  end: dayjs.Dayjs;
};

export type TimeDimensionPredefinedGranularity =
  'second'
  | 'minute'
  | 'hour'
  | 'day'
  | 'week'
  | 'month'
  | 'quarter'
  | 'year';

export type TimeDimensionGranularity = TimeDimensionPredefinedGranularity | string;

export type TGranularityMap = {
  name: TimeDimensionGranularity | undefined;
  title: string;
};

export const GRANULARITIES: TGranularityMap[] = [
  { name: undefined, title: 'w/o grouping' },
  { name: 'second', title: 'Second' },
  { name: 'minute', title: 'Minute' },
  { name: 'hour', title: 'Hour' },
  { name: 'day', title: 'Day' },
  { name: 'week', title: 'Week' },
  { name: 'month', title: 'Month' },
  { name: 'quarter', title: 'Quarter' },
  { name: 'year', title: 'Year' },
];

export const DEFAULT_GRANULARITY = 'day';

// When granularity is week, weekStart Value must be 1. However, since the client can change it globally
// (https://day.js.org/docs/en/i18n/changing-locale) So the function below has been added.
export const internalDayjs = (...args: any[]): dayjs.Dayjs => dayjs(...args).locale({ ...en, weekStart: 1 });

export const TIME_SERIES: Record<string, (range: DayRange) => string[]> = {
  day: (range) => range.by('d').map(d => d.format('YYYY-MM-DDT00:00:00.000')),
  month: (range) => range.snapTo('month').by('M').map(d => d.format('YYYY-MM-01T00:00:00.000')),
  year: (range) => range.snapTo('year').by('y').map(d => d.format('YYYY-01-01T00:00:00.000')),
  hour: (range) => range.by('h').map(d => d.format('YYYY-MM-DDTHH:00:00.000')),
  minute: (range) => range.by('m').map(d => d.format('YYYY-MM-DDTHH:mm:00.000')),
  second: (range) => range.by('s').map(d => d.format('YYYY-MM-DDTHH:mm:ss.000')),
  week: (range) => range.snapTo('week').by('w').map(d => d.startOf('week').format('YYYY-MM-DDT00:00:00.000')),
  quarter: (range) => range.snapTo('quarter').by('quarter').map(d => d.startOf('quarter').format(
    'YYYY-MM-DDT00:00:00.000'
  )),
};

export const isPredefinedGranularity = (granularity: TimeDimensionGranularity): boolean => !!TIME_SERIES[granularity];

export const DateRegex = /^\d\d\d\d-\d\d-\d\d$/;
export const LocalDateRegex = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}Z?$/;

export const dayRange = (from: any, to: any): DayRange => ({
  by: (value: any) => {
    const results = [];

    let start = internalDayjs(from);
    const end = internalDayjs(to);

    while (start.isBefore(end) || start.isSame(end)) {
      results.push(start);
      start = start.add(1, value);
    }

    return results;
  },
  snapTo: (value: any): DayRange => dayRange(internalDayjs(from).startOf(value), internalDayjs(to).endOf(value)),
  start: internalDayjs(from),
  end: internalDayjs(to),
});

/**
 * Parse PostgreSQL-like interval string into object
 * E.g. '2 years 15 months 100 weeks 99 hours 15 seconds'
 * Negative units are also supported
 * E.g. '-2 months 5 days -10 hours'
 *
 * TODO: It's copy/paste of parseSqlInterval from @cubejs-backend/shared [time.ts]
 * It's not referenced to omit imports of moment.js staff.
 * Probably one day we should choose one implementation and reuse it in other places.
 */
export function parseSqlInterval(intervalStr: SqlInterval): ParsedInterval {
  const interval: ParsedInterval = {};
  const parts = intervalStr.split(/\s+/);

  for (let i = 0; i < parts.length; i += 2) {
    const value = parseInt(parts[i], 10);
    const unit = parts[i + 1];

    // Remove ending 's' (e.g., 'days' -> 'day')
    const singularUnit = unit.endsWith('s') ? unit.slice(0, -1) : unit;
    interval[singularUnit] = value;
  }

  return interval;
}

/**
 * Adds interval to provided date.
 * TODO: It's copy/paste of addInterval from @cubejs-backend/shared [time.ts]
 * but operates with dayjs instead of moment.js
 * @param {dayjs} date
 * @param interval
 * @returns {dayjs}
 */
export function addInterval(date: dayjs.Dayjs, interval: ParsedInterval): dayjs.Dayjs {
  let res = date.clone();

  Object.entries(interval).forEach(([key, value]) => {
    res = res.add(value, key);
  });

  return res;
}

/**
 * Adds interval to provided date.
 * TODO: It's copy/paste of subtractInterval from @cubejs-backend/shared [time.ts]
 * but operates with dayjs instead of moment.js
 * @param {dayjs} date
 * @param interval
 * @returns {dayjs}
 */
export function subtractInterval(date: dayjs.Dayjs, interval: ParsedInterval): dayjs.Dayjs {
  let res = date.clone();

  Object.entries(interval).forEach(([key, value]) => {
    res = res.subtract(value, key);
  });

  return res;
}

/**
 * Returns the closest date prior to date parameter aligned with the origin point
 * TODO: It's copy/paste of alignToOrigin from @cubejs-backend/shared [time.ts]
 * but operates with dayjs instead of moment.js
 */
function alignToOrigin(startDate: dayjs.Dayjs, interval: ParsedInterval, origin: dayjs.Dayjs): dayjs.Dayjs {
  let alignedDate = startDate.clone();
  let intervalOp;
  let isIntervalNegative = false;

  let offsetDate = addInterval(origin, interval);

  // The easiest way to check the interval sign
  if (offsetDate.isBefore(origin)) {
    isIntervalNegative = true;
  }

  offsetDate = origin.clone();

  if (startDate.isBefore(origin)) {
    intervalOp = isIntervalNegative ? addInterval : subtractInterval;

    while (offsetDate.isAfter(startDate)) {
      offsetDate = intervalOp(offsetDate, interval);
    }
    alignedDate = offsetDate;
  } else {
    intervalOp = isIntervalNegative ? subtractInterval : addInterval;

    while (offsetDate.isBefore(startDate)) {
      alignedDate = offsetDate.clone();
      offsetDate = intervalOp(offsetDate, interval);
    }

    if (offsetDate.isSame(startDate)) {
      alignedDate = offsetDate;
    }
  }

  return alignedDate;
}

/**
 * Returns the time series points for the custom interval
 * TODO: It's almost a copy/paste of timeSeriesFromCustomInterval from
 * @cubejs-backend/shared [time.ts] but operates with dayjs instead of moment.js
 */
export const timeSeriesFromCustomInterval = (from: string, to: string, granularity: Granularity): string[] => {
  const intervalParsed = parseSqlInterval(granularity.interval);
  const start = internalDayjs(from);
  const end = internalDayjs(to);
  let origin = granularity.origin ? internalDayjs(granularity.origin) : internalDayjs().startOf('year');
  if (granularity.offset) {
    origin = addInterval(origin, parseSqlInterval(granularity.offset));
  }
  let alignedStart = alignToOrigin(start, intervalParsed, origin);

  const dates = [];

  while (alignedStart.isBefore(end) || alignedStart.isSame(end)) {
    dates.push(alignedStart.format('YYYY-MM-DDTHH:mm:ss.000'));
    alignedStart = addInterval(alignedStart, intervalParsed);
  }

  return dates;
};

/**
 * Returns the lowest time unit for the interval
 */
export const diffTimeUnitForInterval = (interval: string): string => {
  if (/second/i.test(interval)) {
    return 'second';
  } else if (/minute/i.test(interval)) {
    return 'minute';
  } else if (/hour/i.test(interval)) {
    return 'hour';
  } else if (/day/i.test(interval)) {
    return 'day';
  } else if (/week/i.test(interval)) {
    return 'day';
  } else if (/month/i.test(interval)) {
    return 'month';
  } else if (/quarter/i.test(interval)) {
    return 'month';
  } else /* if (/year/i.test(interval)) */ {
    return 'year';
  }
};

const granularityOrder = ['year', 'quarter', 'month', 'week', 'day', 'hour', 'minute', 'second'];

export const minGranularityForIntervals = (i1: string, i2: string): string => {
  const g1 = diffTimeUnitForInterval(i1);
  const g2 = diffTimeUnitForInterval(i2);
  const g1pos = granularityOrder.indexOf(g1);
  const g2pos = granularityOrder.indexOf(g2);

  if (g1pos > g2pos) {
    return g1;
  }

  return g2;
};

export const granularityFor = (dateStr: string): string => {
  const dayjsDate = internalDayjs(dateStr);
  const month = dayjsDate.month();
  const date = dayjsDate.date();
  const hours = dayjsDate.hour();
  const minutes = dayjsDate.minute();
  const seconds = dayjsDate.second();
  const milliseconds = dayjsDate.millisecond();
  const weekDay = dayjsDate.isoWeekday();

  if (
    month === 0 &&
    date === 1 &&
    hours === 0 &&
    minutes === 0 &&
    seconds === 0 &&
    milliseconds === 0
  ) {
    return 'year';
  } else if (
    date === 1 &&
    hours === 0 &&
    minutes === 0 &&
    seconds === 0 &&
    milliseconds === 0
  ) {
    return 'month';
  } else if (
    weekDay === 1 &&
    hours === 0 &&
    minutes === 0 &&
    seconds === 0 &&
    milliseconds === 0
  ) {
    return 'week';
  } else if (
    hours === 0 &&
    minutes === 0 &&
    seconds === 0 &&
    milliseconds === 0
  ) {
    return 'day';
  } else if (
    minutes === 0 &&
    seconds === 0 &&
    milliseconds === 0
  ) {
    return 'hour';
  } else if (
    seconds === 0 &&
    milliseconds === 0
  ) {
    return 'minute';
  } else if (
    milliseconds === 0
  ) {
    return 'second';
  }

  return 'second'; // TODO return 'millisecond';
};
