import { TimeDimensionGranularity } from '@cubejs-client/core';
import formatDate from 'date-fns/format';

const FORMAT_MAP = {
  second: 'yyyy-LL-dd HH:mm:ss',
  minute: 'yyyy-LL-dd HH:mm',
  hour: 'yyyy-LL-dd HH:00',
  day: 'yyyy-LL-dd',
  week: "yyyy-LL-dd 'W'w",
  month: 'yyyy LLL',
  quarter: 'yyyy QQQ',
  year: 'yyyy',
};

export function formatDateByGranularity(timestamp: Date, granularity?: TimeDimensionGranularity) {
  return formatDate(timestamp, FORMAT_MAP[granularity ?? 'second']);
}

export function formatDateByPattern(timestamp: Date, format?: string) {
  return formatDate(timestamp, format ?? FORMAT_MAP['second']);
}
