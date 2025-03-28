import { BaseFilter, BaseQuery } from '@cubejs-backend/schema-compiler';

const GRANULARITY_TO_INTERVAL: Record<string, (date: string) => string> = {
  day: date => `DATE_TRUNC('day', ${date})`,
  week: date => `DATE_TRUNC('week', ${date})`,
  hour: date => `DATE_TRUNC('hour', ${date})`,
  minute: date => `DATE_TRUNC('minute', ${date})`,
  second: date => `DATE_TRUNC('second', ${date})`,
  month: date => `DATE_TRUNC('month', ${date})`,
  quarter: date => `DATE_TRUNC('quarter', ${date})`,
  year: date => `DATE_TRUNC('year', ${date})`
};

class DruidFilter extends BaseFilter {
  public likeIgnoreCase(column, not, param, type: string) {
    const p = (!type || type === 'contains' || type === 'ends') ? '%' : '';
    const s = (!type || type === 'contains' || type === 'starts') ? '%' : '';
    return `LOWER(${column})${not ? ' NOT' : ''} LIKE CONCAT('${p}', LOWER(${this.allocateParam(param)}), '${s}')`;
  }
}

export class DruidQuery extends BaseQuery {
  public newFilter(filter) {
    return new DruidFilter(this, filter);
  }

  public timeGroupedColumn(granularity: string, dimension: string) {
    return GRANULARITY_TO_INTERVAL[granularity](dimension);
  }

  public convertTz(field: string) {
    return `CAST(TIME_FORMAT(${field}, 'yyyy-MM-dd HH:mm:ss', '${this.timezone}') AS TIMESTAMP)`;
  }

  public subtractInterval(date: string, interval: string) {
    return `(${date} + INTERVAL ${interval})`;
  }

  public addInterval(date: string, interval: string) {
    return `(${date} + INTERVAL ${interval})`;
  }

  public timeStampCast(value: string) {
    return `TIME_PARSE(${value})`;
  }

  public timeStampParam() {
    return this.timeStampCast('?');
  }

  public nowTimestampSql(): string {
    return 'CURRENT_TIMESTAMP';
  }
}
