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
    return `${field} AT TIME ZONE '${this.timezone}'`;
  }

  public timeStampCast(value: string) {
    return `${value}::timestamptz`;
  }

  public dateTimeCast(value: string) {
    return `${value}::timestampntz`;
  }

  public seriesSql(timeDimension: any) {
    const values = timeDimension.timeSeries().map(
      ([from, to]: [string, string]) => `select '${from}' f, '${to}' t`
    ).join(' UNION ALL ');
    return `SELECT ${this.dateTimeCast('dates.f')} date_from, ${this.dateTimeCast('dates.t')} date_to FROM (${values}) AS dates`;
  }

  public timeGroupedColumn(granularity: string, dimension: string) {
    return `DATE_TRUNC('${GRANULARITY_TO_INTERVAL[granularity]}', ${dimension})`;
  }

  public newFilter(filter: any): BaseFilter {
    return new FireboltFilter(this, filter);
  }
}
