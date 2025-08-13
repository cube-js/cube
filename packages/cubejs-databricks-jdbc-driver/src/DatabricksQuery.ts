import R from 'ramda';
import { BaseFilter, BaseQuery } from '@cubejs-backend/schema-compiler';
import { parseSqlInterval } from '@cubejs-backend/shared';

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

class DatabricksFilter extends BaseFilter {
  public likeIgnoreCase(column: any, not: any, param: any, type: string) {
    const p = (!type || type === 'contains' || type === 'ends') ? '%' : '';
    const s = (!type || type === 'contains' || type === 'starts') ? '%' : '';
    return `LOWER(${column})${not ? ' NOT' : ''} LIKE CONCAT('${p}', LOWER(${this.allocateParam(param)}), '${s}')`;
  }
}

export class DatabricksQuery extends BaseQuery {
  public newFilter(filter: any): BaseFilter {
    return new DatabricksFilter(this, filter);
  }

  public castToString(sql: string): string {
    return `CAST(${sql} as STRING)`;
  }

  public hllInit(sql: string) {
    return `hll_sketch_agg(${sql})`;
  }

  public hllMerge(sql: string) {
    return `hll_union_agg(${sql})`;
  }

  public hllCardinality(sql: string): string {
    return `hll_sketch_estimate(${sql})`;
  }

  public hllCardinalityMerge(sql: string): string {
    return `hll_sketch_estimate(hll_union_agg(${sql}))`;
  }

  public countDistinctApprox(sql: string) {
    return `approx_count_distinct(${sql})`;
  }

  public convertTz(field: string) {
    return `from_utc_timestamp(${field}, '${this.timezone}')`;
  }

  public timeStampCast(value: string) {
    return `from_utc_timestamp(replace(replace(${value}, 'T', ' '), 'Z', ''), 'UTC')`;
  }

  public dateTimeCast(value: string) {
    return `from_utc_timestamp(${value}, 'UTC')`; // TODO
  }

  public subtractInterval(date: string, interval: string): string {
    const intervalParsed = parseSqlInterval(interval);
    let res = date;

    for (const [key, value] of Object.entries(intervalParsed)) {
      res = `(${res} - INTERVAL '${value}' ${key})`;
    }

    return res;
  }

  public addInterval(date: string, interval: string): string {
    const intervalParsed = parseSqlInterval(interval);
    let res = date;

    for (const [key, value] of Object.entries(intervalParsed)) {
      res = `(${res} + INTERVAL '${value}' ${key})`;
    }

    return res;
  }

  public timeGroupedColumn(granularity: string, dimension: string): string {
    return `date_trunc('${GRANULARITY_TO_INTERVAL[granularity]}', ${dimension})`;
  }

  /**
   * Returns sql for source expression floored to timestamps aligned with
   * intervals relative to origin timestamp point.
   */
  public dateBin(interval: string, source: string, origin: string): string {
    const [intervalFormatted, timeUnit] = this.formatInterval(interval);
    const beginOfTime = this.dateTimeCast('\'1970-01-01T00:00:00\'');

    return `${this.dateTimeCast(`'${origin}'`)} + INTERVAL ${intervalFormatted} *
      floor(
        date_diff(${timeUnit}, ${this.dateTimeCast(`'${origin}'`)}, ${source}) /
        date_diff(${timeUnit}, ${beginOfTime}, ${beginOfTime} + INTERVAL ${intervalFormatted})
      )`;
  }

  /**
   * The input interval with (possible) plural units, like "2 years", "3 months", "4 weeks", "5 days"...
   * will be converted to Databricks dialect.
   * @see https://docs.databricks.com/en/sql/language-manual/data-types/interval-type.html
   * It returns a tuple of (formatted interval, timeUnit to use in datediff functions)
   */
  private formatInterval(interval: string): [string, string] {
    const intervalParsed = parseSqlInterval(interval);
    const intKeys = Object.keys(intervalParsed).length;

    if (intervalParsed.year && intKeys === 1) {
      return [`'${intervalParsed.year}' YEAR`, 'YEAR'];
    } else if (intervalParsed.year && intervalParsed.month && intKeys === 2) {
      return [`'${intervalParsed.year}-${intervalParsed.month}' YEAR TO MONTH`, 'MONTH'];
    } else if (intervalParsed.month && intKeys === 1) {
      return [`'${intervalParsed.month}' MONTH`, 'MONTH'];
    } else if (intervalParsed.day && intKeys === 1) {
      return [`'${intervalParsed.day}' DAY`, 'DAY'];
    } else if (intervalParsed.day && intervalParsed.hour && intKeys === 2) {
      return [`'${intervalParsed.day} ${intervalParsed.hour}' DAY TO HOUR`, 'HOUR'];
    } else if (intervalParsed.day && intervalParsed.hour && intervalParsed.minute && intKeys === 3) {
      return [`'${intervalParsed.day} ${intervalParsed.hour}:${intervalParsed.minute}' DAY TO MINUTE`, 'MINUTE'];
    } else if (intervalParsed.day && intervalParsed.hour && intervalParsed.minute && intervalParsed.second && intKeys === 4) {
      return [`'${intervalParsed.day} ${intervalParsed.hour}:${intervalParsed.minute}:${intervalParsed.second}' DAY TO SECOND`, 'SECOND'];
    } else if (intervalParsed.hour && intervalParsed.minute && intKeys === 2) {
      return [`'${intervalParsed.hour}:${intervalParsed.minute}' HOUR TO MINUTE`, 'MINUTE'];
    } else if (intervalParsed.hour && intervalParsed.minute && intervalParsed.second && intKeys === 3) {
      return [`'${intervalParsed.hour}:${intervalParsed.minute}:${intervalParsed.second}' HOUR TO SECOND`, 'SECOND'];
    } else if (intervalParsed.minute && intervalParsed.second && intKeys === 2) {
      return [`'${intervalParsed.minute}:${intervalParsed.second}' MINUTE TO SECOND`, 'SECOND'];
    }

    // No need to support microseconds.

    throw new Error(`Cannot transform interval expression "${interval}" to Databricks dialect`);
  }

  public escapeColumnName(name: string) {
    return `\`${name}\``;
  }

  public override getFieldIndex(id: string): string | number | null {
    const idx = super.getFieldIndex(id);
    if (idx !== null) {
      return idx;
    }

    return this.escapeColumnName(this.aliasName(id, false));
  }

  public unixTimestampSql() {
    return 'unix_timestamp()';
  }

  public defaultRefreshKeyRenewalThreshold() {
    return 120;
  }

  public supportGeneratedSeriesForCustomTd() {
    return true;
  }

  public sqlTemplates() {
    const templates = super.sqlTemplates();
    templates.functions.CURRENTDATE = 'CURRENT_DATE';
    templates.functions.DATETRUNC = 'DATE_TRUNC({{ args_concat }})';
    templates.functions.DATEPART = 'DATE_PART({{ args_concat }})';
    templates.functions.BTRIM = 'TRIM({% if args[1] is defined %}{{ args[1] }} FROM {% endif %}{{ args[0] }})';
    templates.functions.LTRIM = 'LTRIM({{ args|reverse|join(", ") }})';
    templates.functions.RTRIM = 'RTRIM({{ args|reverse|join(", ") }})';
    templates.functions.DATEDIFF = 'DATEDIFF({{ date_part }}, DATE_TRUNC(\'{{ date_part }}\', {{ args[1] }}), DATE_TRUNC(\'{{ date_part }}\', {{ args[2] }}))';
    templates.functions.LEAST = 'LEAST({{ args_concat }})';
    templates.functions.GREATEST = 'GREATEST({{ args_concat }})';
    templates.functions.TRUNC = 'CASE WHEN ({{ args[0] }}) >= 0 THEN FLOOR({{ args_concat }}) ELSE CEIL({{ args_concat }}) END';
    templates.expressions.timestamp_literal = 'from_utc_timestamp(\'{{ value }}\', \'UTC\')';
    templates.expressions.extract = '{% if date_part|lower == "epoch" %}unix_timestamp({{ expr }}){% else %}EXTRACT({{ date_part }} FROM {{ expr }}){% endif %}';
    templates.expressions.interval_single_date_part = 'INTERVAL \'{{ num }}\' {{ date_part }}';
    templates.quotes.identifiers = '`';
    templates.quotes.escape = '``';
    templates.statements.time_series_select = 'SELECT date_from::timestamp AS `date_from`,\n' +
      'date_to::timestamp AS `date_to` \n' +
      'FROM(\n' +
      '    VALUES ' +
      '{% for time_item in seria  %}' +
      '(\'{{ time_item | join(\'\\\', \\\'\') }}\')' +
      '{% if not loop.last %}, {% endif %}' +
      '{% endfor %}' +
      ') AS dates (date_from, date_to)';
    templates.statements.generated_time_series_select = 'SELECT d AS date_from,\n' +
      '(d + INTERVAL {{ granularity }}) - INTERVAL 1 MILLISECOND AS date_to\n' +
      '  FROM (SELECT explode(sequence(\n' +
      '    from_utc_timestamp({{ start }}, \'UTC\'), from_utc_timestamp({{ end }}, \'UTC\'), INTERVAL {{ granularity }}\n' +
      '  )) AS d)';
    templates.statements.generated_time_series_with_cte_range_source =
    'SELECT d AS date_from,\n' +
    '(d + INTERVAL {{ granularity }}) - INTERVAL 1 MILLISECOND AS date_to\n' +
    'FROM {{ range_source }}\n' +
    'LATERAL VIEW explode(\n' +
    '    sequence(\n' +
    '        CAST({{ min_name }} AS TIMESTAMP),\n' +
    '        CAST({{ max_name }} AS TIMESTAMP),\n' +
    '        INTERVAL {{ granularity }}\n' +
    '    )\n' +
    ') dates AS d';

    // TODO: Databricks has `TIMESTAMP_NTZ` with logic similar to Pg's `TIMESTAMP`
    // but that requires Runtime 13.3+. Should this be enabled?
    // templates.types.timestamp = 'TIMESTAMP_NTZ';
    delete templates.types.time;
    // Databricks intervals have a YearMonth or DayTime type variants, but no universal type
    delete templates.types.interval;
    return templates;
  }
}
