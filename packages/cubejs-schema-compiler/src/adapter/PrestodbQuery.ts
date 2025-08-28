import { parseSqlInterval } from '@cubejs-backend/shared';
import { BaseQuery } from './BaseQuery';
import { BaseFilter } from './BaseFilter';

const GRANULARITY_TO_INTERVAL = {
  day: 'day',
  week: 'week',
  hour: 'hour',
  minute: 'minute',
  second: 'second',
  month: 'month',
  quarter: 'quarter',
  year: 'year'
};

class PrestodbFilter extends BaseFilter {
  public likeIgnoreCase(column, not, param, type) {
    const p = (!type || type === 'contains' || type === 'ends') ? '%' : '';
    const s = (!type || type === 'contains' || type === 'starts') ? '%' : '';
    return `LOWER(${column})${not ? ' NOT' : ''} LIKE CONCAT('${p}', LOWER(${this.allocateParam(param)}) , '${s}') ESCAPE '\\'`;
  }

  public castParameter() {
    if (this.definition().type === 'boolean') {
      return 'CAST(? AS BOOLEAN)';
    } else if (this.measure || this.definition().type === 'number') {
      // TODO here can be measure type of string actually
      return 'CAST(? AS DOUBLE)';
    }

    return '?';
  }
}

export class PrestodbQuery extends BaseQuery {
  public newFilter(filter) {
    return new PrestodbFilter(this, filter);
  }

  public timeStampParam() {
    return 'from_iso8601_timestamp(?)';
  }

  public timeStampCast(value) {
    return `from_iso8601_timestamp(${value})`;
  }

  public dateTimeCast(value) {
    return `from_iso8601_timestamp(${value})`;
  }

  public convertTz(field) {
    const atTimezone = `${field} AT TIME ZONE '${this.timezone}'`;
    return this.timezone ?
      `CAST(date_add('minute', timezone_minute(${atTimezone}), date_add('hour', timezone_hour(${atTimezone}), ${field})) AS TIMESTAMP)` :
      field;
  }

  /**
   * Returns sql for source expression floored to timestamps aligned with
   * intervals relative to origin timestamp point.
   * Athena doesn't support INTERVALs directly — using date_diff/date_add
   */
  public dateBin(interval: string, source: string, origin: string): string {
    const intervalParsed = parseSqlInterval(interval);
    const intervalParts = Object.entries(intervalParsed);

    if (intervalParts.length > 1) {
      throw new Error('Athena/Presto supports only simple intervals with one date part');
    }

    const [unit, count] = intervalParts[0];
    const originExpr = this.timeStampCast(`'${origin}'`);

    return `date_add('${unit}',
      floor(
        date_diff('${unit}', ${originExpr}, ${source}) / ${count}
      ) * ${count},
      ${originExpr}
    )`;
  }

  public timeGroupedColumn(granularity, dimension) {
    return `date_trunc('${GRANULARITY_TO_INTERVAL[granularity]}', ${dimension})`;
  }

  public intervalString(interval: string): string {
    const [intervalValue, intervalUnit] = interval.split(' ');
    return `'${intervalValue}' ${intervalUnit}`;
  }

  public seriesSql(timeDimension) {
    const values = timeDimension.timeSeries().map(
      ([from, to]) => `select '${from}' f, '${to}' t`
    ).join(' UNION ALL ');
    return `SELECT from_iso8601_timestamp(dates.f) date_from, from_iso8601_timestamp(dates.t) date_to FROM (${values}) AS dates`;
  }

  public unixTimestampSql() {
    return `to_unixtime(${this.nowTimestampSql()})`;
  }

  public defaultRefreshKeyRenewalThreshold() {
    return 120;
  }

  public defaultEveryRefreshKey() {
    return {
      every: '2 minutes'
    };
  }

  public hllInit(sql) {
    return `cast(approx_set(${sql}) as varbinary)`;
  }

  public hllMerge(sql) {
    return `cardinality(merge(cast(${sql} as HyperLogLog)))`;
  }

  public countDistinctApprox(sql) {
    return `approx_distinct(${sql})`;
  }

  public supportGeneratedSeriesForCustomTd() {
    return true;
  }

  protected limitOffsetClause(limit, offset) {
    const limitClause = limit != null ? ` LIMIT ${limit}` : '';
    const offsetClause = offset != null ? ` OFFSET ${offset}` : '';
    return `${offsetClause}${limitClause}`;
  }

  public sqlTemplates() {
    const templates = super.sqlTemplates();
    templates.functions.DATETRUNC = 'DATE_TRUNC({{ args_concat }})';
    templates.functions.DATEPART = 'DATE_PART({{ args_concat }})';
    templates.functions.DATEDIFF = 'DATE_DIFF(\'{{ date_part }}\', {{ args[1] }}, {{ args[2] }})';
    templates.functions.CURRENTDATE = 'CURRENT_DATE';
    delete templates.functions.PERCENTILECONT;
    templates.statements.select = '{% if ctes %} WITH \n' +
          '{{ ctes | join(\',\n\') }}\n' +
          '{% endif %}' +
      'SELECT {% if distinct %}DISTINCT {% endif %}{{ select_concat | map(attribute=\'aliased\') | join(\', \') }}  {% if from %}\n' +
      'FROM (\n  {{ from }}\n) AS {{ from_alias }} {% elif from_prepared %}\n' +
      'FROM {{ from_prepared }}' +
      '{% endif %}' +
      '{% if filter %}\nWHERE {{ filter }}{% endif %}' +
      '{% if group_by %} GROUP BY {{ group_by }}{% endif %}' +
      '{% if having %}\nHAVING {{ having }}{% endif %}' +
      '{% if order_by %} ORDER BY {{ order_by | map(attribute=\'expr\') | join(\', \') }}{% endif %}' +
      '{% if offset is not none %}\nOFFSET {{ offset }}{% endif %}' +
      '{% if limit is not none %}\nLIMIT {{ limit }}{% endif %}';
    templates.expressions.extract = 'EXTRACT({{ date_part }} FROM {{ expr }})';
    templates.expressions.interval_single_date_part = 'INTERVAL \'{{ num }}\' {{ date_part }}';
    templates.expressions.timestamp_literal = 'from_iso8601_timestamp(\'{{ value }}\')';
    // Presto requires concat types to be VARCHAR
    templates.expressions.binary = '{% if op == \'||\' %}' +
      '(CAST({{ left }} AS VARCHAR) || CAST({{ right }} AS VARCHAR))' +
      '{% else %}({{ left }} {{ op }} {{ right }}){% endif %}';
    delete templates.expressions.ilike;
    templates.types.string = 'VARCHAR';
    templates.types.float = 'REAL';
    // Presto intervals have a YearMonth or DayTime type variants, but no universal type
    delete templates.types.interval;
    templates.types.binary = 'VARBINARY';
    templates.tesseract.ilike = 'LOWER({{ expr }}) {% if negated %}NOT {% endif %} LIKE {{ pattern }}';
    templates.tesseract.bool_param_cast = 'CAST({{ expr }} AS BOOLEAN)';
    templates.tesseract.number_param_cast = 'CAST({{ expr }} AS DOUBLE)';
    templates.filters.like_pattern = 'CONCAT({% if start_wild %}\'%\'{% else %}\'\'{% endif %}, LOWER({{ value }}), {% if end_wild %}\'%\'{% else %}\'\'{% endif %}) ESCAPE \'\\\'';
    templates.statements.time_series_select = 'SELECT from_iso8601_timestamp(dates.f) date_from, from_iso8601_timestamp(dates.t) date_to \n' +
    'FROM (\n' +
    '{% for time_item in seria  %}' +
    '    select \'{{ time_item[0] }}\' f, \'{{ time_item[1] }}\' t \n' +
    '{% if not loop.last %} UNION ALL\n{% endif %}' +
    '{% endfor %}' +
    ') AS dates';
    templates.statements.generated_time_series_select = 'SELECT d AS date_from,\n' +
    'date_add(\'MILLISECOND\', -1, d + interval {{ granularity }}) AS date_to\n' +
    'FROM UNNEST(\n' +
    'SEQUENCE(CAST(from_iso8601_timestamp({{ start }}) AS TIMESTAMP), CAST(from_iso8601_timestamp({{ end }}) AS TIMESTAMP), INTERVAL {{ granularity }})\n' +
    ') AS dates(d)';
    templates.statements.generated_time_series_with_cte_range_source = 'SELECT d AS date_from,\n' +
    'date_add(\'MILLISECOND\', -1, d + interval {{ granularity }}) AS date_to\n' +
    'FROM {{ range_source }} CROSS JOIN UNNEST(\n' +
    'SEQUENCE(CAST({{ range_source }}.{{ min_name }} AS TIMESTAMP), CAST({{ range_source }}.{{ max_name }} AS TIMESTAMP), INTERVAL {{ granularity }})\n' +
    ') AS dates(d)';
    return templates;
  }

  public castToString(sql: any): string {
    return `CAST(${sql} as VARCHAR)`;
  }
}
