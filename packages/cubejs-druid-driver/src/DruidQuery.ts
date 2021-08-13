import moment from 'moment-timezone';
import { BaseFilter, BaseQuery } from '@cubejs-backend/schema-compiler';

const GRANULARITY_TO_INTERVAL: Record<string, (date: string) => string> = {
  day: date => `DATE_TRUNC('day', ${date})`,
  week: date => `DATE_TRUNC('week', ${date})`,
  hour: date => `DATE_TRUNC('hour', ${date})`,
  minute: date => `DATE_TRUNC('minute', ${date})`,
  second: date => `DATE_TRUNC('second', ${date})`,
  month: date => `DATE_TRUNC('month', ${date})`,
  year: date => `DATE_TRUNC('year', ${date})`
};

class DruidFilter extends BaseFilter {
  public likeIgnoreCase(column, not, param) {
    return `${column}${not ? ' NOT' : ''} LIKE CONCAT('%', ${this.allocateParam(param)}, '%')`;
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
    // TODO respect day light saving
    const [hour, minute] = moment().tz(this.timezone).format('Z').split(':');
    const minutes = parseInt(hour, 10) * 60 + parseInt(minute, 10);

    if (minutes > 0) {
      return `TIMESTAMPADD(MINUTES, ${minutes}, ${field})`;
    }

    return field;
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
    return `CURRENT_TIMESTAMP`;
  }
}
