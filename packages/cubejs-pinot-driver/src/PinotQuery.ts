import { BaseFilter, BaseQuery, BaseTimeDimension } from '@cubejs-backend/schema-compiler';
import { parseSqlInterval } from '@cubejs-backend/shared';

enum GRANULARITY_TO_INTERVAL {
  day = 'day',
  week = 'week',
  hour = 'hour',
  minute = 'minute',
  second = 'second',
  month = 'month',
  quarter = 'quarter',
  year = 'year'
}

type GRANULARITY_ID = keyof typeof GRANULARITY_TO_INTERVAL;

const DATE_TIME_FORMAT = '\'yyyy-MM-dd HH:mm:ss.SSS\'';

// Pinot has no `date + INTERVAL '...'` syntax. Fixed-length units are added to a
// timestamp as epoch-millis offsets produced by these `fromEpoch<Unit>()` functions
// (note: pluralized and case-sensitive). Calendar units (month/quarter/year), whose
// length varies, must instead go through TIMESTAMPADD().
const PINOT_EPOCH_FN: Record<string, string> = {
  second: 'fromEpochSeconds',
  minute: 'fromEpochMinutes',
  hour: 'fromEpochHours',
  day: 'fromEpochDays',
  week: 'fromEpochDays',
};

class PinotTimeDimension extends BaseTimeDimension {
  public formatFromDate(date: string) {
    return super.formatFromDate(date).replace('T', ' ');
  }

  public formatToDate(date: string) {
    return super.formatToDate(date).replace('T', ' ');
  }

  public timeSeries(): string[][] {
    if (!this.granularity) return super.timeSeries();

    return super.timeSeries().map(([from, to]) => ([from.replace('T', ' '), to.replace('T', ' ')]));
  }
}

class PinotFilter extends BaseFilter {
  public likeIgnoreCase(column: any, not: any, param: any, type: string) {
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

export class PinotQuery extends BaseQuery {
  public newFilter(filter: any): PinotFilter {
    return new PinotFilter(this, filter);
  }

  public timeStampParam() {
    return '?';
  }

  public timeStampCast(value: string) {
    return `CAST(${value} as TIMESTAMP)`;
  }

  public dateTimeCast(value: string) {
    return value;
  }

  public convertTz(field: string) {
    return this.timeStampCast(`toDateTime(${field}, ${DATE_TIME_FORMAT}, '${this.timezone}')`);
  }

  public timeGroupedColumn(granularity: GRANULARITY_ID, dimension: string) {
    return this.timeStampCast(`dateTrunc('${GRANULARITY_TO_INTERVAL[granularity]}', ${dimension})`);
  }

  public subtractInterval(date: string, interval: string) {
    return this.applyInterval(date, interval, -1);
  }

  public addInterval(date: string, interval: string) {
    return this.applyInterval(date, interval, 1);
  }

  /**
   * Renders Pinot-valid interval arithmetic on top of a timestamp expression.
   * The interval string (e.g. "7 day", "1 year", "1 year 2 month") is parsed into
   * its unit parts; fixed-length units are applied as epoch-millis offsets via
   * fromEpoch<Unit>(), calendar units via TIMESTAMPADD(). `sign` is +1 for addition
   * and -1 for subtraction.
   */
  private applyInterval(date: string, interval: string, sign: number): string {
    const parsed = parseSqlInterval(interval);
    let expr = this.timeStampCast(date);

    for (const [unit, rawValue] of Object.entries(parsed)) {
      const value = (rawValue as number) * sign;

      switch (unit) {
        case 'second':
        case 'minute':
        case 'hour':
        case 'day':
        case 'week': {
          const amount = unit === 'week' ? value * 7 : value;
          const op = amount < 0 ? '-' : '+';
          expr = `${expr} ${op} ${PINOT_EPOCH_FN[unit]}(${Math.abs(amount)})`;
          break;
        }
        case 'month':
          expr = `TIMESTAMPADD(MONTH, ${value}, ${expr})`;
          break;
        case 'quarter':
          expr = `TIMESTAMPADD(MONTH, ${value * 3}, ${expr})`;
          break;
        case 'year':
          expr = `TIMESTAMPADD(YEAR, ${value}, ${expr})`;
          break;
        default:
          throw new Error(`Unsupported interval unit "${unit}" for the Pinot dialect`);
      }
    }

    return expr;
  }

  public seriesSql(timeDimension: BaseTimeDimension) {
    const values = timeDimension.timeSeries().map(
      ([from, to]) => `select '${from}' f, '${to}' t`
    ).join(' UNION ALL ');
    return `SELECT ${this.timeStampCast('dates.f')} date_from, ${this.timeStampCast('dates.t')} date_to
            FROM (${values}) AS dates`;
  }

  public applyMeasureFilters(evaluateSql: '*' | string, symbol: any, cubeName: string) {
    if (!symbol.filters || !symbol.filters.length) {
      return evaluateSql;
    }

    const where = this.evaluateMeasureFilters(symbol, cubeName);

    return `${evaluateSql === '*' ? '1' : evaluateSql}) FILTER (WHERE ${where}`;
  }

  /**
   * @return {string}
   */
  public timestampFormat() {
    return 'YYYY-MM-DD HH:mm:ss.SSS';
  }

  public unixTimestampSql() {
    return this.nowTimestampSql();
  }

  public defaultRefreshKeyRenewalThreshold() {
    return 120;
  }

  public defaultEveryRefreshKey() {
    return {
      every: '2 minutes'
    };
  }

  public countDistinctApprox(sql: string) {
    return `DistinctCountHLLPlus(${sql})`;
  }

  protected limitOffsetClause(limit: string | number, offset: string | number) {
    const limitClause = limit != null ? ` LIMIT ${limit}` : '';
    const offsetClause = offset != null ? ` OFFSET ${offset}` : '';
    // Pinot expects LIMIT before OFFSET.
    return `${limitClause}${offsetClause}`;
  }

  public newTimeDimension(timeDimension: any): BaseTimeDimension {
    return new PinotTimeDimension(this, timeDimension);
  }

  public sqlTemplates() {
    const templates = super.sqlTemplates();
    templates.functions.DATETRUNC = 'DATE_TRUNC({{ args_concat }})';
    // NOW() returns the current epoch millis (inherently UTC), matching the
    // epoch-millis representation produced by the timestamp_literal template
    templates.functions.UTCTIMESTAMP = 'NOW()';
    templates.functions.STRING_AGG = 'LISTAGG({% if distinct %}DISTINCT {% endif %}{{ args_concat }})';
    templates.statements.select = '{% if ctes %} WITH \n' +
      '{{ ctes | join(\',\n\') }}\n' +
      '{% endif %}' +
      'SELECT {% if distinct %}DISTINCT {% endif %}' +
      '{{ select_concat | map(attribute=\'aliased\') | join(\', \') }} {% if from %}\n' +
      'FROM (\n' +
      '{{ from | indent(2, true) }}\n' +
      ') AS {{ from_alias }}{% elif from_prepared %}\n' +
      'FROM {{ from_prepared }}' +
      '{% endif %}' +
      '{% for join in joins %}\n{{ join }}{% endfor %}' +
      '{% if filter %}\nWHERE {{ filter }}{% endif %}' +
      '{% if group_by %}\nGROUP BY {{ group_by }}{% endif %}' +
      '{% if having %}\nHAVING {{ having }}{% endif %}' +
      '{% if order_by %}\nORDER BY {{ order_by | map(attribute=\'expr\') | join(\', \') }}{% endif %}' +
      // Pinot (multi-stage engine) expects LIMIT before OFFSET; the reverse order is rejected.
      '{% if limit is not none %}\nLIMIT {{ limit }}{% endif %}' +
      '{% if offset is not none %}\nOFFSET {{ offset }}{% endif %}';
    templates.expressions.extract = 'EXTRACT({{ date_part }} FROM {{ expr }})';
    templates.expressions.timestamp_literal = `fromDateTime('{{ value }}', ${DATE_TIME_FORMAT})`;
    // NOTE: this template contains a comma; two order expressions are being generated
    templates.expressions.sort = '{{ expr }} IS NULL {% if nulls_first %}DESC{% else %}ASC{% endif %}, {{ expr }} {% if asc %}ASC{% else %}DESC{% endif %}';
    templates.expressions.ilike = 'LOWER({{ expr }}) {% if negated %}NOT {% endif %}LIKE LOWER({{ pattern }})';
    templates.filters.like_pattern = 'CONCAT({% if start_wild %}\'%\'{% else %}\'\'{% endif %}, LOWER({{ value }}), {% if end_wild %}\'%\'{% else %}\'\'{% endif %})';
    templates.tesseract.ilike = 'LOWER({{ expr }}) {% if negated %}NOT {% endif %} LIKE {{ pattern }}';
    templates.tesseract.series_bounds_cast = 'CAST({{ expr }} AS TIMESTAMP)';
    templates.expressions.rolling_window_expr_timestamp_cast = 'CAST({{ value }} AS TIMESTAMP)';
    templates.statements.time_series_select = 'SELECT CAST(f AS TIMESTAMP) date_from, CAST(t AS TIMESTAMP) date_to \n' +
      'FROM (\n' +
      '{% for time_item in seria %}' +
      '    select \'{{ time_item[0] }}\' f, \'{{ time_item[1] }}\' t \n' +
      '{% if not loop.last %} UNION ALL\n{% endif %}' +
      '{% endfor %}' +
      ') AS dates';
    templates.quotes.identifiers = '"';
    delete templates.types.time;
    delete templates.types.interval;
    delete templates.types.binary;
    return templates;
  }
}
