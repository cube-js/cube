import { BaseQuery } from '@cubejs-backend/schema-compiler';
import moment from 'moment-timezone';

const GRANULARITY_TO_INTERVAL: Record<string, (date: string) => string> = {
  second: (date) => `DATEADD(second, DATEDIFF(second, CAST('1900-01-01' as TIMESTAMP), ${date}), CAST('1900-01-01' as TIMESTAMP))`,
  minute: (date) => `DATEADD(minute, DATEDIFF(minute, CAST('1900-01-01' as TIMESTAMP), ${date}), CAST('1900-01-01' as TIMESTAMP))`,
  hour: (date) => `DATEADD(hour, DATEDIFF(hour, CAST('1900-01-01' as TIMESTAMP), ${date}), CAST('1900-01-01' as TIMESTAMP))`,
  day: (date) => `DATEADD(day, DATEDIFF(day, CAST('1900-01-01' as TIMESTAMP), ${date}), CAST('1900-01-01' as TIMESTAMP))`,
  week: (date) => `DATEADD(week, DATEDIFF(week, CAST('1900-01-01' as TIMESTAMP), ${date}), CAST('1900-01-01' as TIMESTAMP))`,
  month: (date) => `DATEADD(month, DATEDIFF(month, CAST('1900-01-01' as TIMESTAMP), ${date}), CAST('1900-01-01' as TIMESTAMP))`,
  quarter: (date) => `DATEADD(quarter, DATEDIFF(quarter, CAST('1900-01-01' as TIMESTAMP), ${date}), CAST('1900-01-01' as TIMESTAMP))`,
  year: (date) => `DATEADD(year, DATEDIFF(year, CAST('1900-01-01' as TIMESTAMP), ${date}), CAST('1900-01-01' as TIMESTAMP))`,
};

export class MongoDBDriverQuery extends BaseQuery {
  public timeStampCast(value: string) {
    return `CAST(${value} as TIMESTAMP)`;
  }

  public dateTimeCast(value: string) {
    return `CAST(${value} AS TIMESTAMP)`;
  }

  public groupByClause() {
    if (this.ungrouped) {
      return '';
    }

    const names = this.dimensionAliasNames();

    return names.length ? ` GROUP BY ${names.join(', ')}` : '';
  }

  public timeGroupedColumn(granularity: string, dimension: string) {
    return GRANULARITY_TO_INTERVAL[granularity](dimension);
  }

  public convertTz(field: string) {
    const offset = moment().tz(this.timezone).utcOffset();
    const hours = Math.sign(offset) * Math.floor(Math.abs(offset) / 60);
    const minutes = offset % 60;

    let result = field;

    if (hours !== 0) {
      result = `TIMESTAMPADD(HOUR, ${hours}, ${result})`;
    }

    if (minutes !== 0) {
      result = `TIMESTAMPADD(MINUTE, ${minutes}, ${result})`;
    }

    return result;
  }

  public nowTimestampSql() {
    return 'CURRENT_TIMESTAMP';
  }

  public unixTimestampSql() {
    return 'DATEDIFF(second, CAST(\'1970-01-01\'), CURRENT_TIMESTAMP)';
  }
}
