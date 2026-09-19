import { BaseQuery } from './BaseQuery';
import { BaseFilter } from './BaseFilter';

const GRANULARITY_TO_INTERVAL = {
  day: 'DAY',
  week: 'WEEK',
  hour: 'HOUR',
  minute: 'MINUTE',
  second: 'SECOND',
  month: 'MONTH',
  quarter: 'QUARTER',
  year: 'YEAR'
};

// Ordered from the smallest, so the first match is the interval's own unit.
const INTERVAL_UNITS = ['second', 'minute', 'hour', 'day', 'week', 'month', 'quarter', 'year'];

class SnowflakeFilter extends BaseFilter {
  public likeIgnoreCase(column: string, not: boolean, param: any, type: string) {
    const p = (!type || type === 'contains' || type === 'ends') ? '\'%\' || ' : '';
    const s = (!type || type === 'contains' || type === 'starts') ? ' || \'%\'' : '';
    // From Snowflake docs:
    // If you use the backslash as an escape character, then you must escape the backslash in both the expression
    // and the ESCAPE clause. For example, the following command specifies that the escape character is the backslash,
    // and then uses that escape character to search for % as a literal (without the escape character, the % would be
    // treated as a wildcard): `'SOMETHING%' ILIKE '%\\%%' ESCAPE '\\';`
    //
    // Special chars in bind vars are escaped with backslash which in turn is also escaped by backslash.
    // To get double backslashes passed inside generated SQL string we need to escape each one.
    // That is why here are FOUR backslashes.
    return `${column}${not ? ' NOT' : ''} ILIKE ${p}${this.allocateParam(param)}${s} ESCAPE '\\\\'`;
  }
}

export class SnowflakeQuery extends BaseQuery {
  public newFilter(filter) {
    return new SnowflakeFilter(this, filter);
  }

  public convertTz(field) {
    return `CONVERT_TIMEZONE('${this.timezone}', ${field}::timestamp_tz)::timestamp_ntz`;
  }

  public timeGroupedColumn(granularity, dimension) {
    return `date_trunc('${GRANULARITY_TO_INTERVAL[granularity]}', ${dimension})`;
  }

  /**
   * Returns sql for source expression floored to timestamps aligned with
   * intervals relative to origin timestamp point.
   */
  public dateBin(interval: string, source: string, origin: string): string {
    const intervalFormatted = this.formatInterval(interval);
    const timeUnit = this.diffTimeUnitForInterval(interval);
    const beginOfTime = 'TIMESTAMP_FROM_PARTS(1970, 1, 1, 0, 0, 0)';

    return `DATEADD(${timeUnit},
        FLOOR(
          DATEDIFF(${timeUnit}, ${this.dateTimeCast(`'${origin}'`)}, ${source}) /
          DATEDIFF(${timeUnit}, ${beginOfTime}, (${beginOfTime} + interval '${intervalFormatted}'))
        ) * DATEDIFF(${timeUnit}, ${beginOfTime}, (${beginOfTime} + interval '${intervalFormatted}')),
        ${this.dateTimeCast(`'${origin}'`)})`;
  }

  public subtractInterval(date: string, interval: string): string {
    return `${date} - interval '${this.formatInterval(interval)}'`;
  }

  public addInterval(date: string, interval: string): string {
    return `${date} + interval '${this.formatInterval(interval)}'`;
  }

  /**
   * The input interval in format "2 years 3 months 4 weeks 5 days...."
   * will be converted to Snowflake dialect "2 years, 3 months, 4 weeks, 5 days...."
   */
  private formatInterval(interval: string): string {
    return interval.split(' ').map((word, index, arr) => {
      if (index % 2 !== 0 && index < arr.length - 1) {
        return `${word},`;
      }
      return word;
    }).join(' ');
  }

  public timeStampCast(value) {
    return `${value}::timestamp_tz`;
  }

  /**
   * The generated time series steps with DATEADD, which takes a time unit and a
   * row number rather than an interval, so it needs the unit the interval is
   * actually expressed in. `diffTimeUnitForInterval` answers a different
   * question - it degrades WEEK to DAY and QUARTER to MONTH, which DATEADD would
   * then step by, producing seven or three times as many periods as asked for.
   *
   * Multi-unit intervals reach this through calendar granularities, which read
   * the interval and ignore the unit, so an unrecognized one falls back rather
   * than throwing.
   */
  public override intervalAndMinimalTimeUnit(interval: string): [string, string] {
    const unit = INTERVAL_UNITS.find(u => new RegExp(`\\b${u}s?\\b`, 'i').test(interval));

    return [interval, unit || 'year'];
  }

  public defaultRefreshKeyRenewalThreshold() {
    return 120;
  }

  public defaultEveryRefreshKey() {
    return {
      every: '2 minutes'
    };
  }

  public nowTimestampSql() {
    return 'CURRENT_TIMESTAMP';
  }

  public hllInit(sql) {
    return `HLL_EXPORT(HLL_ACCUMULATE(${sql}))`;
  }

  public hllMerge(sql) {
    return `HLL_ESTIMATE(HLL_COMBINE(HLL_IMPORT(${sql})))`;
  }

  public countDistinctApprox(sql) {
    return `APPROX_COUNT_DISTINCT(${sql})`;
  }

  public sqlTemplates() {
    const templates = super.sqlTemplates();
    templates.functions.DATETRUNC = 'DATE_TRUNC({{ args_concat }})';
    templates.functions.DATEPART = 'DATE_PART({{ args_concat }})';
    templates.functions.CURRENTDATE = 'CURRENT_DATE';
    templates.functions.NOW = 'CURRENT_TIMESTAMP';
    templates.functions.UTCTIMESTAMP = 'SYSDATE()';
    templates.functions.LOG = 'LOG({% if args[1] is undefined %}10, {% endif %}{{ args_concat }})';
    templates.functions.DLOG10 = 'LOG(10, {{ args_concat }})';
    templates.functions.CHARACTERLENGTH = 'LENGTH({{ args[0] }})';
    templates.functions.BTRIM = 'TRIM({{ args_concat }})';
    templates.functions.STRING_AGG = 'LISTAGG({% if distinct %}DISTINCT {% endif %}{{ args_concat }})';
    // DATEADD is being rewritten to DATE_ADD
    templates.functions.DATE_ADD = 'DATEADD({{ date_part }}, {{ interval }}, {{ args[0] }})';
    templates.expressions.extract = 'EXTRACT({{ date_part }} FROM {{ expr }})';
    // Snowflake `/` is decimal division even for integer operands (output scale
    // is dividend scale + 6), while this template must keep PostgreSQL integer
    // division semantics. TRUNC rounds toward zero, matching PostgreSQL.
    templates.expressions.int_division = 'CAST(TRUNC({{ left }} / {{ right }}) AS BIGINT)';
    // Snowflake can't EXTRACT(EPOCH FROM <interval>), so the epoch of a timestamp
    // difference (left - right) is rendered as fractional seconds between them.
    // TIMESTAMPDIFF is measured once at microsecond granularity (no per-second
    // boundary rounding) and divided to seconds, matching Postgres' fractional
    // EXTRACT(EPOCH FROM interval).
    templates.expressions.extract_epoch_diff = 'TIMESTAMPDIFF(MICROSECOND, {{ right }}, {{ left }}) / 1000000';
    templates.expressions.interval = 'INTERVAL \'{{ interval }}\'';
    templates.expressions.timestamp_literal = '\'{{ value }}\'::timestamp_tz';
    templates.expressions.like = '{{ expr }} {% if negated %}NOT {% endif %}LIKE {{ pattern }}{% if default_escape %} ESCAPE \'\\\\\'{% endif %}';
    templates.expressions.ilike = '{{ expr }} {% if negated %}NOT {% endif %}ILIKE {{ pattern }}{% if default_escape %} ESCAPE \'\\\\\'{% endif %}';
    templates.operators.is_not_distinct_from = 'IS NOT DISTINCT FROM';
    // Snowflake has no default LIKE escape character, so the escaping the
    // planner applies to the value needs an explicit clause - the same one
    // SnowflakeFilter.likeIgnoreCase emits on the legacy path, and doubled for
    // the same reason described there.
    templates.tesseract.ilike = '{{ expr }} {% if negated %}NOT {% endif %}ILIKE {{ pattern }} ESCAPE \'\\\\\'';
    templates.tesseract.join_types_full = 'FULL';
    // ARRAY_GENERATE_RANGE is the only Snowflake row generator whose bounds may
    // be expressions rather than a literal count. Its row number counts whole
    // time units, all DATEADD can step by, so `supportGeneratedSeriesForCustomTd`
    // stays off.
    //
    // DATEDIFF counts unit boundaries crossed, so it over-allocates rows for a
    // range that does not begin on one; the trailing WHERE, not the count, is
    // what ends the series where the range does.
    //
    // Unquoted identifiers fold to upper case here, so both output columns are
    // quoted. The series stays timestamp_ntz to match the time dimension, which
    // `timeStampCast` would not - hence the unused date_from/date_to arguments.
    templates.statements.generated_time_series_select = 'SELECT series_date AS "date_from",\n' +
      'DATEADD(MILLISECOND, -1, DATEADD({{ minimal_time_unit }}, 1, series_date)) AS "date_to"\n' +
      'FROM (SELECT DATEADD({{ minimal_time_unit }}, series_index.value::int, {{ start }}::timestamp_ntz) AS series_date\n' +
      'FROM TABLE(FLATTEN(input => ARRAY_GENERATE_RANGE(0, DATEDIFF({{ minimal_time_unit }}, {{ start }}::timestamp_ntz, {{ end }}::timestamp_ntz) + 1))) AS series_index) AS series\n' +
      'WHERE series_date <= {{ end }}::timestamp_ntz';
    templates.statements.generated_time_series_with_cte_range_source = 'SELECT series_date AS "date_from",\n' +
      'DATEADD(MILLISECOND, -1, DATEADD({{ minimal_time_unit }}, 1, series_date)) AS "date_to"\n' +
      'FROM (SELECT DATEADD({{ minimal_time_unit }}, series_index.value::int, {{ range_source }}."{{ min_name }}") AS series_date,\n' +
      '{{ range_source }}."{{ max_name }}" AS series_end\n' +
      'FROM {{ range_source }}, LATERAL FLATTEN(input => ARRAY_GENERATE_RANGE(0, DATEDIFF({{ minimal_time_unit }}, {{ range_source }}."{{ min_name }}", {{ range_source }}."{{ max_name }}") + 1)) AS series_index) AS series\n' +
      'WHERE series_date <= series_end';
    delete templates.types.interval;
    return templates;
  }
}
