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

class DruidFilter extends BaseFilter {
  /**
   * Druid SQL is Calcite-based and has no default LIKE escape character, so the ESCAPE clause
   * is required for BaseFilter.escapeWildcardChars to have any effect
   */
  public likeIgnoreCase(column, not, param, type: string) {
    const p = (!type || type === 'contains' || type === 'ends') ? '%' : '';
    const s = (!type || type === 'contains' || type === 'starts') ? '%' : '';
    return `LOWER(${column})${not ? ' NOT' : ''} LIKE CONCAT('${p}', LOWER(${this.allocateParam(param)}), '${s}') ESCAPE '\\'`;
  }
}

export class DruidQuery extends BaseQuery {
  public newFilter(filter) {
    return new DruidFilter(this, filter);
  }

  public timeGroupedColumn(granularity: string, dimension: string) {
    return GRANULARITY_TO_INTERVAL[granularity](dimension);
  }

  public convertTz(field: string) {
    return `CAST(TIME_FORMAT(${field}, 'yyyy-MM-dd HH:mm:ss', '${this.timezone}') AS TIMESTAMP)`;
  }

  public subtractInterval(date: string, interval: string) {
    return `(${date} + INTERVAL ${interval})`;
  }

  public addInterval(date: string, interval: string) {
    return `(${date} + INTERVAL ${interval})`;
  }

  public timeStampCast(value: string) {
    return `TIME_PARSE(${value})`;
  }

  public timeStampParam() {
    return this.timeStampCast('?');
  }

  public nowTimestampSql(): string {
    return 'CURRENT_TIMESTAMP';
  }

  public sqlTemplates() {
    const templates = super.sqlTemplates();
    // Druid has neither ILIKE nor a default LIKE escape character, so mirror
    // DruidFilter.likeIgnoreCase: case insensitivity comes from LOWER() on both sides and
    // `filters.like_escape_char` only takes effect through an explicit ESCAPE clause, which
    // binds to the whole right operand and so cannot sit inside LOWER().
    templates.tesseract.ilike = 'LOWER({{ expr }}) {% if negated %}NOT {% endif %}LIKE {{ pattern }}';
    templates.filters.like_pattern = 'CONCAT({% if start_wild %}\'%\'{% else %}\'\'{% endif %}, LOWER({{ value }}), {% if end_wild %}\'%\'{% else %}\'\'{% endif %}) ESCAPE \'\\\'';
    return templates;
  }
}
