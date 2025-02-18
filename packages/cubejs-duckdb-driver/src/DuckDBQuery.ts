import { BaseFilter, BaseQuery } from '@cubejs-backend/schema-compiler';

const GRANULARITY_TO_INTERVAL: Record<string, (date: string) => string> = {
  day: date => `DATE_TRUNC('day', ${date})`,
  week: date => `DATE_TRUNC('week', ${date})`,
  hour: date => `DATE_TRUNC('hour', ${date})`,
  minute: date => `DATE_TRUNC('minute', ${date})`,
  second: date => `DATE_TRUNC('second', ${date})`,
  month: date => `DATE_TRUNC('month', ${date})`,
  quarter: date => `DATE_TRUNC('quarter', ${date})`,
  year: date => `DATE_TRUNC('year', ${date})`
};

class DuckDBFilter extends BaseFilter {
}

export class DuckDBQuery extends BaseQuery {
  public newFilter(filter: any): BaseFilter {
    return new DuckDBFilter(this, filter);
  }

  public convertTz(field: string) {
    return `timezone('${this.timezone}', ${field}::timestamptz)`;
  }

  public timeGroupedColumn(granularity: string, dimension: string) {
    return GRANULARITY_TO_INTERVAL[granularity](dimension);
  }

  /**
   * Returns sql for source expression floored to timestamps aligned with
   * intervals relative to origin timestamp point.
   * DuckDB operates with whole intervals as is without measuring them in plain seconds,
   * so the resulting date will be human-expected aligned with intervals.
   */
  public dateBin(interval: string, source: string, origin: string): string {
    const timeUnit = this.diffTimeUnitForInterval(interval);
    const beginOfTime = this.dateTimeCast('\'1970-01-01 00:00:00.000\'');

    return `${this.dateTimeCast(`'${origin}'`)}' + INTERVAL '${interval}' *
      floor(
        date_diff('${timeUnit}', ${this.dateTimeCast(`'${origin}'`)}, ${source}) /
        date_diff('${timeUnit}', ${beginOfTime}, ${beginOfTime} + INTERVAL '${interval}')
      )::int`;
  }

  public countDistinctApprox(sql: string) {
    return `approx_count_distinct(${sql})`;
  }

  public sqlTemplates() {
    const templates = super.sqlTemplates();
    templates.functions.DATETRUNC = 'DATE_TRUNC({{ args_concat }})';
    templates.functions.LEAST = 'LEAST({{ args_concat }})';
    templates.functions.GREATEST = 'GREATEST({{ args_concat }})';
    return templates;
  }
}
