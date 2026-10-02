import { BaseQuery } from './BaseQuery';
import type { BaseDimension } from './BaseDimension';

const GRANULARITY_TO_INTERVAL: Record<string, string> = {
  day: 'DD',
  week: 'W',
  hour: 'HH24',
  minute: 'mm',
  second: 'ss',
  month: 'MM',
  quarter: 'Q',
  year: 'YY'
};

export class VerticaQuery extends BaseQuery {
  public convertTz(field: string) {
    return `${field} AT TIME ZONE '${this.timezone}'`;
  }

  // eslint-disable-next-line no-unused-vars
  public timeStampParam(timeDimension: BaseDimension) {
    return this.timeStampCast('?');
  }

  public timeGroupedColumn(granularity: string, dimension: string): string {
    return `TRUNC(${dimension}, '${GRANULARITY_TO_INTERVAL[granularity]}')`;
  }
}
