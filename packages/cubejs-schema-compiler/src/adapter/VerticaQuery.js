import { BaseQuery } from './BaseQuery';

const GRANULARITY_TO_INTERVAL = {
  day: 'DD',
  week: 'W',
  hour: 'HH24',
  minute: 'mm',
  second: 'ss',
  month: 'MM',
  year: 'YY'
};

export class VerticaQuery extends BaseQuery {
  convertTz(field) {
    return `${field} AT TIME ZONE '${this.timezone}'`;
  }

  // eslint-disable-next-line no-unused-vars
  timeStampParam(timeDimension) {
    return this.timeStampCast('?');
  }

  timeGroupedColumn(granularity, dimension) {
    return `TRUNC(${dimension}, '${GRANULARITY_TO_INTERVAL[granularity]}')`;
  }
}
