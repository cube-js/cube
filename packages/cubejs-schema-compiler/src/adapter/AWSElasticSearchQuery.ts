import { BaseFilter } from './BaseFilter';
import { ElasticSearchQuery } from './ElasticSearchQuery';

const GRANULARITY_TO_INTERVAL = {
  day: (date) => `DATE_FORMAT(${date}, 'yyyy-MM-dd 00:00:00.000')`,
  // eslint-disable-next-line no-unused-vars,@typescript-eslint/no-unused-vars
  week: (date) => { throw new Error('Week is unsupported'); }, // TODO
  hour: (date) => `DATE_FORMAT(${date}, 'yyyy-MM-dd HH:00:00.000')`,
  minute: (date) => `DATE_FORMAT(${date}, 'yyyy-MM-dd HH:mm:00.000')`,
  second: (date) => `DATE_FORMAT(${date}, 'yyyy-MM-dd HH:mm:ss.000')`,
  month: (date) => `DATE_FORMAT(${date}, 'yyyy-MM-01 00:00:00.000')`,
  year: (date) => `DATE_FORMAT(${date}, 'yyyy-01-01 00:00:00.000')`
};

class AWSElasticSearchQueryFilter extends BaseFilter {
  public likeIgnoreCase(column, not, param, type) {
    const p = (!type || type === 'contains' || type === 'ends') ? '%' : '';
    const s = (!type || type === 'contains' || type === 'starts') ? '%' : '';
    return `${column}${not ? ' NOT' : ''} LIKE CONCAT('${p}', ${this.allocateParam(param)}, '${s}')`;
  }
}

export class AWSElasticSearchQuery extends ElasticSearchQuery {
  public override newFilter(filter) {
    return new AWSElasticSearchQueryFilter(this, filter);
  }

  public override subtractInterval(date, interval) {
    return `DATE_SUB(${date}, INTERVAL ${interval})`;
  }

  public override addInterval(date, interval) {
    return `DATE_ADD(${date}, INTERVAL ${interval})`;
  }

  public override timeGroupedColumn(granularity, dimension) {
    return GRANULARITY_TO_INTERVAL[granularity](dimension);
  }

  public override unixTimestampSql() {
    return 'EXTRACT(EPOCH FROM NOW())';
  }
}
