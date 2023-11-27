import moment from 'moment-timezone';

import { MysqlQuery } from './MysqlQuery';

export class MongoBiQuery extends MysqlQuery {
  public convertTz(field: string): string {
    const tz = moment().tz(this.timezone);
    // TODO respect day light saving
    const [hour, minute] = tz.format('Z').split(':');
    const [hourInt, minuteInt] = [parseInt(hour, 10), parseInt(minute, 10) * Math.sign(parseInt(hour, 10))];
    let result = field;
    if (hourInt !== 0) {
      result = `TIMESTAMPADD(HOUR, ${hourInt}, ${result})`;
    }
    if (minuteInt !== 0) {
      result = `TIMESTAMPADD(MINUTE, ${minuteInt}, ${result})`;
    }
    return result;
  }

  public timeStampCast(value: string): string {
    return `TIMESTAMP(${value})`;
  }
}
