import formatDate from 'date-fns/format';

export function formatDateByPattern(timestamp: Date, format?: string) {
  return formatDate(timestamp, format ?? 'yyyy-LL-dd HH:mm:ss');
}
