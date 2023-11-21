import { BaseFilter, BaseQuery } from '@cubejs-backend/schema-compiler';

type Granularity = 'day' | 'week' | 'hour' | 'minute' | 'second' | 'month' | 'quarter' | 'year';

// duckdb timestamptz is interperted as string by cubejs
const dateTrunc = (granularity: Granularity, date: string) => `DATE_TRUNC('${granularity}', ${date})::timestamp`;

const GRANULARITY_TO_INTERVAL: Record<Granularity, (date: string) => string> = {
  day: date => dateTrunc('day', date),
  week: date => dateTrunc('week', date),
  hour: date => dateTrunc('hour', date),
  minute: date => dateTrunc('minute', date),
  second: date => dateTrunc('second', date),
  month: date => dateTrunc('month', date),
  quarter: date => dateTrunc('quarter', date),
  year: date => dateTrunc('year', date),
};

class DuckDBFilter extends BaseFilter {
}

export class DuckDBQuery extends BaseQuery {
  public newFilter(filter: any): DuckDBFilter {
    return new DuckDBFilter(this, filter);
  }

  public convertTz(field: string): string {
    return `timezone('${this.timezone}', ${field}::timestamptz)`;
  }

  public timeGroupedColumn(granularity: Granularity, dimension: string): string {
    if (!GRANULARITY_TO_INTERVAL[granularity]) {
      throw new Error(`Unrecognized granularity: ${granularity}`);
    }

    return GRANULARITY_TO_INTERVAL[granularity](dimension);
  }

  public countDistinctApprox(sql: string): string {
    return `approx_count_distinct(${sql})`;
  }

  public sqlTemplates() {
    const templates = super.sqlTemplates();
    templates.functions.DATETRUNC = 'DATE_TRUNC({{ args_concat }})';
    return templates;
  }
}
