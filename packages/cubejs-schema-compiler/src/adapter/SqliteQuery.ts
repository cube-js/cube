import moment from 'moment-timezone';

import { BaseQuery } from './BaseQuery';
import { BaseFilter } from './BaseFilter';

const GRANULARITY_TO_INTERVAL = {
  day: (date) => `strftime('%Y-%m-%dT00:00:00.000', ${date})`,
  week: (date) => `strftime('%Y-%m-%dT00:00:00.000', CASE WHEN date(${date}, 'weekday 1') = date(${date}) THEN date(${date}, 'weekday 1') ELSE date(${date}, 'weekday 1', '-7 days') END)`,
  hour: (date) => `strftime('%Y-%m-%dT%H:00:00.000', ${date})`,
  minute: (date) => `strftime('%Y-%m-%dT%H:%M:00.000', ${date})`,
  second: (date) => `strftime('%Y-%m-%dT%H:%M:%S.000', ${date})`,
  month: (date) => `strftime('%Y-%m-01T00:00:00.000', ${date})`,
  year: (date) => `strftime('%Y-01-01T00:00:00.000', ${date})`
};

class SqliteFilter extends BaseFilter {
  public likeIgnoreCase(column, not, param, type) {
    const p = (!type || type === 'contains' || type === 'ends') ? '\'%\' || ' : '';
    const s = (!type || type === 'contains' || type === 'starts') ? ' || \'%\'' : '';
    return `${column}${not ? ' NOT' : ''} LIKE ${p}${this.allocateParam(param)}${s} COLLATE NOCASE`;
  }
}

export class SqliteQuery extends BaseQuery {
  public newFilter(filter) {
    return new SqliteFilter(this, filter);
  }

  public convertTz(field) {
    return `${this.timeStampCast(field)} || '${
      moment().tz(this.timezone).format('Z')
        .replace('-', '+')
        .replace('+', '-')
    }'`;
  }

  public floorSql(numeric) {
    // SQLite doesnt support FLOOR
    return `(CAST((${numeric}) as int) - ((${numeric}) < CAST((${numeric}) as int)))`;
  }

  public timeStampCast(value) {
    return `strftime('%Y-%m-%dT%H:%M:%f', ${value})`;
  }

  public dateTimeCast(value) {
    return `strftime('%Y-%m-%dT%H:%M:%f', ${value})`;
  }

  public subtractInterval(date, interval) {
    return `strftime('%Y-%m-%dT%H:%M:%f', ${date}, '${interval.replace('-', '+').replace(/(^\+|^)/, '-')}')`;
  }

  public addInterval(date, interval) {
    return `strftime('%Y-%m-%dT%H:%M:%f', ${date}, '${interval}')`;
  }

  public timeGroupedColumn(granularity, dimension) {
    return GRANULARITY_TO_INTERVAL[granularity](dimension);
  }

  public seriesSql(timeDimension) {
    const values = timeDimension.timeSeries().map(
      ([from, to]) => `select '${from}' f, '${to}' t`
    ).join(' UNION ALL ');
    return `SELECT dates.f date_from, dates.t date_to FROM (${values}) AS dates`;
  }

  public nowTimestampSql() {
    // eslint-disable-next-line quotes
    return `strftime('%Y-%m-%dT%H:%M:%fZ', 'now')`;
  }

  public unixTimestampSql() {
    // eslint-disable-next-line quotes
    return `strftime('%s','now')`;
  }
}
