import moment from 'moment-timezone';
import BaseQuery from '@cubejs-backend/schema-compiler/adapter/BaseQuery';

const GRANULARITY_TO_INTERVAL: Record<string, (date: string) => string> = {
  day: date => `DATE_TRUNC('day', ${date})`,
  week: date => `DATE_TRUNC('week', ${date})`,
  hour: date => `DATE_TRUNC('hour', ${date})`,
  minute: date => `DATE_TRUNC('minute', ${date})`,
  second: date => `DATE_TRUNC('second', ${date})`,
  month: date => `DATE_TRUNC('month', ${date})`,
  year: date => `DATE_TRUNC('year', ${date})`
};

export class DruidQuery extends BaseQuery {
  timeGroupedColumn(granularity: string, dimension: string) {
    return GRANULARITY_TO_INTERVAL[granularity](dimension);
  }

  convertTz(field: string) {
    // TODO respect day light saving
    const [hour, minute] = moment().tz(this.timezone).format('Z').split(':');
    const minutes = parseInt(hour, 10) * 60 + parseInt(minute, 10);

    if (minutes > 0) {
      return `TIMESTAMPADD(MINUTES, ${minutes}, ${field})`;
    }

    return field;
  }

  subtractInterval(date: string, interval: string) {
    return `(${date} + INTERVAL ${interval})`;
  }

  addInterval(date: string, interval: string) {
    return `(${date} + INTERVAL ${interval})`;
  }

  timeStampCast(value: string) {
    return `TIME_PARSE(${value})`;
  }

  timeStampParam() {
    return this.timeStampCast('?');
  }
}
