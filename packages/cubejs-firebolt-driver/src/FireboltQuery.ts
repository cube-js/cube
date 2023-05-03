import { BaseFilter, BaseQuery, ParamAllocator } from '@cubejs-backend/schema-compiler';

const GRANULARITY_TO_INTERVAL: Record<string, string> = {
  day: 'DAY',
  week: 'WEEK',
  hour: 'HOUR',
  minute: 'MINUTE',
  second: 'SECOND',
  month: 'MONTH',
  quarter: 'QUARTER',
  year: 'YEAR'
};

class FireboltFilter extends BaseFilter {
  public castParameter() {
    if (this.definition().type === 'boolean') {
      return 'CAST(? AS BOOLEAN)';
    }

    return '?';
  }
}

export class FireboltQuery extends BaseQuery {
  public paramAllocator!: ParamAllocator;

  public convertTz(field: string) {
    return field;
  }

  public timeStampCast(value: string) {
    return `${value}::timestamp`;
  }

  public timeGroupedColumn(granularity: string, dimension: string) {
    return `DATE_TRUNC('${GRANULARITY_TO_INTERVAL[granularity]}', ${dimension})`;
  }

  public newFilter(filter: any) {
    return new FireboltFilter(this, filter);
  }
}
