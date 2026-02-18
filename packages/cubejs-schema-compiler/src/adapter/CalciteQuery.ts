import { BaseQuery } from './BaseQuery';
import { BaseFilter } from './BaseFilter';

const GRANULARITY_TO_INTERVAL: Record<string, string> = {
  day: 'day',
  week: 'week',
  hour: 'hour',
  minute: 'minute',
  second: 'second',
  month: 'month',
  quarter: 'quarter',
  year: 'year'
};

class CalciteFilter extends BaseFilter {
  /**
   * Override conditionSql to fix $ character handling in column names.
   * The base implementation uses String.replace() which interprets $` and $'
   * as special patterns, corrupting column names that contain $.
   */
  public conditionSql(columnSql) {
    const operatorMethod = `${this.camelizeOperator}Where`;

    let sql = this[operatorMethod](columnSql);
    if (this.query.paramAllocator.hasParametersInSql(sql)) {
      return sql;
    }

    sql = this[operatorMethod]('$$$COLUMN$$$');
    return this.query.paramAllocator
      .allocateParamsForQuestionString(sql, this.filterParams())
      .replace(/\$\$\$COLUMN\$\$\$/g, () => columnSql);
  }

  public likeIgnoreCase(column: string, not: boolean, param, type: string) {
    const p = (!type || type === 'contains' || type === 'ends') ? '%' : '';
    const s = (!type || type === 'contains' || type === 'starts') ? '%' : '';
    return `${column}${not ? ' NOT' : ''} LIKE CONCAT('${p}', ${this.allocateParam(param)}, '${s}')`;
  }
}

export class CalciteQuery extends BaseQuery {
  public newFilter(filter) {
    return new CalciteFilter(this, filter);
  }

  public convertTz(field: string): string {
    return field;
  }

  public timeGroupedColumn(granularity: string, dimension: string): string {
    return `date_trunc('${GRANULARITY_TO_INTERVAL[granularity]}', ${dimension})`;
  }

  public dateBin(interval: string, source: string, origin: string): string {
    const originTs = `TIMESTAMP '${origin}'`;
    return `(${originTs} + (FLOOR(EXTRACT(EPOCH FROM (${source} - ${originTs})) / EXTRACT(EPOCH FROM INTERVAL '${interval}')) * INTERVAL '${interval}'))`;
  }

  public timeStampCast(value: string): string {
    if (typeof value === 'string') {
      const literalMatch = value.match(/^'(.+)'$/);
      if (literalMatch) {
        const normalized = literalMatch[1]
          .replace('T', ' ')
          .replace(/\.\d+$/, '');
        return `TIMESTAMP '${normalized}'`;
      }
    }
    return `CAST(${value} AS TIMESTAMP)`;
  }

  public dateTimeCast(value: string): string {
    return `CAST(${value} AS TIMESTAMP)`;
  }

  public castToString(sql: string): string {
    return `CAST(${sql} AS VARCHAR)`;
  }

  public countDistinctApprox(sql: string): string {
    return `COUNT(DISTINCT ${sql})`;
  }

  public escapeColumnName(name: string): string {
    return `\`${name}\``;
  }

  public concatStringsSql(strings: string[]): string {
    return `CONCAT(${strings.join(', ')})`;
  }

  public seriesSql(timeDimension): string {
    const values = timeDimension.timeSeries().map(
      ([from, to]) => `select '${from}' f, '${to}' t`
    ).join(' UNION ALL ');
    return `SELECT CAST(dates.f AS TIMESTAMP) date_from, CAST(dates.t AS TIMESTAMP) date_to FROM (${values}) AS dates`;
  }

  public unixTimestampSql(): string {
    return 'UNIX_TIMESTAMP()';
  }

  public wrapSegmentForDimensionSelect(sql: string): string {
    return `CASE WHEN ${sql} THEN 1 ELSE 0 END`;
  }

  public sqlTemplates() {
    const templates = super.sqlTemplates();

    templates.quotes.identifiers = '`';
    templates.quotes.escape = '\\`';

    templates.functions.DATETRUNC = 'DATE_TRUNC({{ args_concat }})';
    templates.functions.DATEPART = 'EXTRACT({{ args_concat }})';
    templates.functions.CURRENTDATE = 'CURRENT_DATE';
    templates.functions.CONCAT = 'CONCAT({{ args_concat }})';
    templates.functions.STRING_AGG = 'GROUP_CONCAT({% if distinct %}DISTINCT {% endif %}{{ args[0] }} SEPARATOR {{ args[1] }})';
    delete templates.functions.PERCENTILECONT;

    templates.expressions.extract = 'EXTRACT({{ date_part }} FROM {{ expr }})';
    templates.expressions.interval_single_date_part = 'INTERVAL \'{{ num }}\' {{ date_part }}';
    templates.expressions.timestamp_literal = 'TIMESTAMP \'{{ value }}\'';
    templates.expressions.concat_strings = 'CONCAT({{ strings | join(\',\' ) }})';

    delete templates.expressions.ilike;

    templates.tesseract.ilike = 'LOWER({{ expr }}) {% if negated %}NOT {% endif %}LIKE {{ pattern }}';

    templates.filters.like_pattern = 'CONCAT({% if start_wild %}\'%\'{% else %}\'\'{% endif %}, LOWER({{ value }}), {% if end_wild %}\'%\'{% else %}\'\'{% endif %})';

    templates.types.string = 'VARCHAR';
    templates.types.boolean = 'BOOLEAN';
    templates.types.timestamp = 'TIMESTAMP';
    templates.types.binary = 'VARBINARY';
    delete templates.types.interval;

    templates.statements.time_series_select = 'SELECT CAST(dates.f AS TIMESTAMP) date_from, CAST(dates.t AS TIMESTAMP) date_to \n' +
      'FROM (\n' +
      '{% for time_item in seria  %}' +
      '    select \'{{ time_item[0] }}\' f, \'{{ time_item[1] }}\' t \n' +
      '{% if not loop.last %} UNION ALL\n{% endif %}' +
      '{% endfor %}' +
      ') AS dates';

    return templates;
  }
}
