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

  public sqlTemplates() {
    const templates = super.sqlTemplates();
    // The BI Connector speaks a MySQL 5.7-era dialect and documents no OVER clause at all,
    // so none of the base window functions can render SQL it will run. Leaving them defined
    // sends it a syntax error; removing them computes the window in Cube instead.
    delete templates.functions.LAG;
    delete templates.functions.LEAD;
    delete templates.functions.ROW_NUMBER;
    delete templates.functions.RANK;
    delete templates.functions.DENSE_RANK;
    delete templates.functions.PERCENT_RANK;
    delete templates.functions.CUME_DIST;
    delete templates.functions.NTILE;
    delete templates.functions.FIRST_VALUE;
    delete templates.functions.LAST_VALUE;
    delete templates.functions.NTH_VALUE;
    return templates;
  }
}
