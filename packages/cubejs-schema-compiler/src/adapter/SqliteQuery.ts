import moment from 'moment-timezone';
import { splitSqlInterval } from '@cubejs-backend/shared';

import { BaseQuery } from './BaseQuery';
import { BaseFilter } from './BaseFilter';
import { BaseTimeDimension } from './BaseTimeDimension';

const GRANULARITY_TO_INTERVAL: Record<string, (date: string) => string> = {
  day: (date: string) => `strftime('%Y-%m-%dT00:00:00.000', ${date})`,
  week: (date: string) => `strftime('%Y-%m-%dT00:00:00.000', CASE WHEN date(${date}, 'weekday 1') = date(${date}) THEN date(${date}, 'weekday 1') ELSE date(${date}, 'weekday 1', '-7 days') END)`,
  hour: (date: string) => `strftime('%Y-%m-%dT%H:00:00.000', ${date})`,
  minute: (date: string) => `strftime('%Y-%m-%dT%H:%M:00.000', ${date})`,
  second: (date: string) => `strftime('%Y-%m-%dT%H:%M:%S.000', ${date})`,
  month: (date: string) => `strftime('%Y-%m-01T00:00:00.000', ${date})`,
  year: (date: string) => `strftime('%Y-01-01T00:00:00.000', ${date})`,
  quarter: (date: string) => `CASE
      WHEN cast(strftime('%m', ${date}) as integer) BETWEEN 1 AND 3 THEN strftime('%Y-01-01T00:00:00.000', ${date})
      WHEN cast(strftime('%m', ${date}) as integer) BETWEEN 4 AND 6 THEN strftime('%Y-04-01T00:00:00.000', ${date})
      WHEN cast(strftime('%m', ${date}) as integer) BETWEEN 7 AND 9 THEN strftime('%Y-07-01T00:00:00.000', ${date})
      ELSE strftime('%Y-10-01T00:00:00.000', ${date})
    END`
};

class SqliteFilter extends BaseFilter {
  public likeIgnoreCase(column: string, not: boolean, param: unknown, type: string) {
    const p = (!type || type === 'contains' || type === 'ends') ? '\'%\' || ' : '';
    const s = (!type || type === 'contains' || type === 'starts') ? ' || \'%\'' : '';
    return `${column}${not ? ' NOT' : ''} LIKE ${p}${this.allocateParam(param)}${s} COLLATE NOCASE`;
  }
}

export class SqliteQuery extends BaseQuery {
  public newFilter(filter: any) {
    return new SqliteFilter(this, filter);
  }

  public convertTz(field: string) {
    return `${this.timeStampCast(field)} || '${
      moment().tz(this.timezone).format('Z')
        .replace('-', '+')
        .replace('+', '-')
    }'`;
  }

  public floorSql(numeric: string) {
    // SQLite doesnt support FLOOR
    return `(CAST((${numeric}) as int) - ((${numeric}) < CAST((${numeric}) as int)))`;
  }

  public timeStampCast(value: string) {
    return `strftime('%Y-%m-%dT%H:%M:%f', ${value})`;
  }

  public dateTimeCast(value: string) {
    return `strftime('%Y-%m-%dT%H:%M:%f', ${value})`;
  }

  public subtractInterval(date: string, interval: string) {
    return this.applyInterval(
      date,
      splitSqlInterval(interval).map(part => part.replace('-', '+').replace(/(^\+|^)/, '-'))
    );
  }

  public addInterval(date: string, interval: string) {
    return this.applyInterval(date, splitSqlInterval(interval));
  }

  /**
   * A strftime modifier carries a single unit, so a compound interval becomes one modifier
   * argument per unit, coarsest first.
   */
  private applyInterval(date: string, parts: string[]): string {
    const modifiers = parts.map(part => `'${part}'`).join(', ');

    return `strftime('%Y-%m-%dT%H:%M:%f', ${date}, ${modifiers})`;
  }

  public timeGroupedColumn(granularity: string, dimension: string) {
    return GRANULARITY_TO_INTERVAL[granularity](dimension);
  }

  public seriesSql(timeDimension: BaseTimeDimension) {
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

  public sqlTemplates() {
    const templates = super.sqlTemplates();
    delete templates.functions.WIDTH_BUCKET;
    // A compound select takes no parenthesised operands in SQLite, so the SQL API leaves
    // set operations to post processing here rather than pushing them down.
    delete templates.statements.union;
    return templates;
  }
}
