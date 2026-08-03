import { BaseFilter, BaseQuery } from '@cubejs-backend/schema-compiler';

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

  public sqlTemplates() {
    const templates = super.sqlTemplates();
    // Timestamp constants arrive as ISO-8601 UTC strings ('2021-01-01T00:00:00.000Z');
    // the TIMESTAMPTZ literal accepts a 'T' separator and the 'Z' UTC designator per
    // Firebolt's documented ISO-8601/RFC-3339 grammar. The base template renders the
    // value bare, which is invalid syntax
    templates.expressions.timestamp_literal = 'TIMESTAMPTZ \'{{ value }}\'';
    templates.tesseract.bool_param_cast = 'CAST({{ expr }} AS BOOLEAN)';
    return templates;
  }

  public defaultRefreshKeyRenewalThreshold() {
    return 120;
  }

  public defaultEveryRefreshKey() {
    return {
      every: '2 minutes'
    };
  }
}
