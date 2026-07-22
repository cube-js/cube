import { parseSqlInterval } from '@cubejs-backend/shared';
import { BaseQuery } from './BaseQuery';
import { BaseFilter } from './BaseFilter';
import { UserError } from '../compiler/UserError';
import type { BaseDimension } from './BaseDimension';

const GRANULARITY_VALUE = {
  day: 'DD',
  week: 'IW',
  hour: 'HH24',
  minute: 'mm',
  second: 'ss',
  month: 'MM',
  quarter: 'Q',
  year: 'YYYY'
};

class OracleFilter extends BaseFilter {
  public castParameter() {
    return ':"?"';
  }

  /**
   * "ILIKE" is not supported
   */
  public likeIgnoreCase(column, not, param, type) {
    const p = (!type || type === 'contains' || type === 'ends') ? '\'%\' || ' : '';
    const s = (!type || type === 'contains' || type === 'starts') ? ' || \'%\'' : '';
    return `${column}${not ? ' NOT' : ''} LIKE ${p}${this.allocateParam(param)}${s} ESCAPE '\\'`;
  }
}

export class OracleQuery extends BaseQuery {
  /**
   * "LIMIT" on Oracle is illegal
   * TODO replace with limitOffsetClause override
   */
  public groupByDimensionLimit() {
    const limitClause = this.rowLimit === null ? '' : ` FETCH NEXT ${this.rowLimit && parseInt(this.rowLimit, 10) || 10000} ROWS ONLY`;
    const offsetClause = this.offset ? ` OFFSET ${parseInt(this.offset, 10)} ROWS` : '';
    return `${offsetClause}${limitClause}`;
  }

  /**
   * "AS" for table aliasing on Oracle it's illegal
   */
  public get asSyntaxTable() {
    return '';
  }

  public get asSyntaxJoin() {
    return this.asSyntaxTable;
  }

  /**
   * Oracle doesn't support group by index,
   * using forSelect dimensions for grouping
   */
  public groupByClause() {
    // Only include dimensions that have select columns
    // Time dimensions without granularity return null from selectColumns()
    const dimensions = this.forSelect().filter((item: any) => (
      !!item.dimension && item.selectColumns && item.selectColumns()
    )) as BaseDimension[];
    if (!dimensions.length) {
      return '';
    }

    return ` GROUP BY ${dimensions.map(item => item.dimensionSql()).join(', ')}`;
  }

  public convertTz(field) {
    /**
     * TODO: add offset timezone
     */
    return field;
  }

  public dateTimeCast(value) {
    // Use timezone-aware parsing for ISO 8601 with milliseconds and trailing 'Z', then cast to DATE
    // to preserve index-friendly comparisons against DATE columns.
    return `CAST(TO_TIMESTAMP_TZ(:"${value}", 'YYYY-MM-DD"T"HH24:MI:SS.FF"Z"') AS DATE)`;
  }

  public timeStampCast(value) {
    // Return timezone-aware timestamp for TIMESTAMP comparisons
    return `TO_TIMESTAMP_TZ(:"${value}", 'YYYY-MM-DD"T"HH24:MI:SS.FF"Z"')`;
  }

  public timeStampParam(timeDimension) {
    return timeDimension.dateFieldType() === 'string' ? ':"?"' : this.timeStampCast('?');
  }

  public timeGroupedColumn(granularity, dimension) {
    if (!granularity) {
      return dimension;
    }

    return `TRUNC(${dimension}, '${GRANULARITY_VALUE[granularity]}')`;
  }

  /**
   * Oracle uses ADD_MONTHS for year/month/quarter intervals
   * and NUMTODSINTERVAL for day/hour/minute/second intervals
   */
  public addInterval(date: string, interval: string): string {
    const intervalParsed = parseSqlInterval(interval);
    let res = date;

    // Handle year/month/quarter using ADD_MONTHS
    let totalMonths = 0;
    if (intervalParsed.year) {
      totalMonths += intervalParsed.year * 12;
    }
    if (intervalParsed.quarter) {
      totalMonths += intervalParsed.quarter * 3;
    }
    if (intervalParsed.month) {
      totalMonths += intervalParsed.month;
    }

    if (totalMonths !== 0) {
      res = `ADD_MONTHS(${res}, ${totalMonths})`;
    }

    // Handle day/hour/minute/second using NUMTODSINTERVAL
    if (intervalParsed.day) {
      res = `${res} + NUMTODSINTERVAL(${intervalParsed.day}, 'DAY')`;
    }
    if (intervalParsed.hour) {
      res = `${res} + NUMTODSINTERVAL(${intervalParsed.hour}, 'HOUR')`;
    }
    if (intervalParsed.minute) {
      res = `${res} + NUMTODSINTERVAL(${intervalParsed.minute}, 'MINUTE')`;
    }
    if (intervalParsed.second) {
      res = `${res} + NUMTODSINTERVAL(${intervalParsed.second}, 'SECOND')`;
    }

    return res;
  }

  /**
   * Oracle subtraction uses ADD_MONTHS with negative values
   * and subtracts NUMTODSINTERVAL for time units
   */
  public subtractInterval(date: string, interval: string): string {
    const intervalParsed = parseSqlInterval(interval);
    let res = date;

    // Handle year/month/quarter using ADD_MONTHS with negative values
    let totalMonths = 0;
    if (intervalParsed.year) {
      totalMonths += intervalParsed.year * 12;
    }
    if (intervalParsed.quarter) {
      totalMonths += intervalParsed.quarter * 3;
    }
    if (intervalParsed.month) {
      totalMonths += intervalParsed.month;
    }

    if (totalMonths !== 0) {
      res = `ADD_MONTHS(${res}, -${totalMonths})`;
    }

    // Handle day/hour/minute/second using NUMTODSINTERVAL with subtraction
    if (intervalParsed.day) {
      res = `${res} - NUMTODSINTERVAL(${intervalParsed.day}, 'DAY')`;
    }
    if (intervalParsed.hour) {
      res = `${res} - NUMTODSINTERVAL(${intervalParsed.hour}, 'HOUR')`;
    }
    if (intervalParsed.minute) {
      res = `${res} - NUMTODSINTERVAL(${intervalParsed.minute}, 'MINUTE')`;
    }
    if (intervalParsed.second) {
      res = `${res} - NUMTODSINTERVAL(${intervalParsed.second}, 'SECOND')`;
    }

    return res;
  }

  public dateBin(interval: string, source: string, origin: string): string {
    const parsed = parseSqlInterval(interval);
    const originTs = `TO_TIMESTAMP('${origin}', 'YYYY-MM-DD"T"HH24:MI:SS.FF3')`;

    const totalMonths = (parsed.year || 0) * 12 + (parsed.quarter || 0) * 3 + (parsed.month || 0);
    const totalSeconds = (parsed.week || 0) * 604800 + (parsed.day || 0) * 86400 +
      (parsed.hour || 0) * 3600 + (parsed.minute || 0) * 60 + (parsed.second || 0);

    // Pure month-based interval: bin with calendar-accurate month arithmetic.
    if (totalMonths > 0 && totalSeconds === 0) {
      return `ADD_MONTHS(${originTs}, FLOOR(MONTHS_BETWEEN(${source}, ${originTs}) / ${totalMonths}) * ${totalMonths})`;
    }

    // Pure fixed-length interval: bin with second arithmetic.
    // (CAST(... AS DATE) - CAST(... AS DATE)) yields a day count; * 86400 → seconds.
    if (totalSeconds > 0 && totalMonths === 0) {
      const diffSeconds = `(CAST(${source} AS DATE) - CAST(${originTs} AS DATE)) * 86400`;
      return `${originTs} + NUMTODSINTERVAL(FLOOR(${diffSeconds} / ${totalSeconds}) * ${totalSeconds}, 'SECOND')`;
    }

    throw new UserError(`Mixed month/second intervals are not supported for Oracle custom granularities: ${interval}`);
  }

  public seriesSql(timeDimension) {
    const values = timeDimension.timeSeries().map(
      ([from, to]) => `SELECT '${from}' f, '${to}' t FROM DUAL`
    ).join(' UNION ALL ');
    return `SELECT TO_TIMESTAMP(dates.f, 'YYYY-MM-DD"T"HH24:MI:SS.FF3') as ${this.escapeColumnName('date_from')}, ` +
      `TO_TIMESTAMP(dates.t, 'YYYY-MM-DD"T"HH24:MI:SS.FF3') as ${this.escapeColumnName('date_to')} ` +
      `FROM (${values}) dates`;
  }

  public sqlTemplates() {
    const templates = super.sqlTemplates();
    templates.functions.UTCTIMESTAMP = 'SYS_EXTRACT_UTC(SYSTIMESTAMP)';
    // Oracle forbids `AS` before a table/subquery alias.
    templates.expressions.query_aliased = '{{ query }} {{ quoted_alias }}';
    // Oracle `/` on NUMBER keeps the fractional part; TRUNC drops decimal digits
    // (truncation toward zero), matching PostgreSQL integer division
    templates.expressions.int_division = 'TRUNC({{ left }} / {{ right }})';
    // Timestamp constants arrive as ISO-8601 UTC strings ('2021-01-01T00:00:00.000Z');
    // the 'T'/'Z' markers are consumed as literal chunks in the format mask. The base
    // template renders the value bare, which is invalid Oracle syntax
    templates.expressions.timestamp_literal = 'TO_TIMESTAMP(\'{{ value }}\', \'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"\')';
    // Oracle does not support positional GROUP BY — group by expressions.
    templates.statements.group_by_exprs = '{{ group_by | map(attribute=\'expr\') | join(\', \') }}';
    // No `AS` before the FROM subquery alias, and Oracle row-limiting syntax.
    templates.statements.select = '{% if ctes %} WITH \n' +
      '{{ ctes | join(\',\n\') }}\n' +
      '{% endif %}' +
      'SELECT {% if distinct %}DISTINCT {% endif %}' +
      '{{ select_concat | map(attribute=\'aliased\') | join(\', \') }} {% if from %}\n' +
      'FROM (\n' +
      '{{ from | indent(2, true) }}\n' +
      ') {{ from_alias }}{% elif from_prepared %}\n' +
      'FROM {{ from_prepared }}' +
      '{% endif %}' +
      '{% for join in joins %}\n{{ join }}{% endfor %}' +
      '{% if filter %}\nWHERE {{ filter }}{% endif %}' +
      '{% if group_by %}\nGROUP BY {{ group_by }}{% endif %}' +
      '{% if having %}\nHAVING {{ having }}{% endif %}' +
      '{% if order_by %}\nORDER BY {{ order_by | map(attribute=\'expr\') | join(\', \') }}{% endif %}' +
      '{% if offset is not none %}\nOFFSET {{ offset }} ROWS{% endif %}' +
      '{% if limit is not none %}\nFETCH NEXT {{ limit }} ROWS ONLY{% endif %}';
    // Oracle has no `::` cast and no `VALUES` row-constructor table source. Build the
    // series with TO_TIMESTAMP + UNION ALL SELECT ... FROM DUAL, and no `AS` before
    // the derived-table alias (Oracle forbids it). `seria` items are [from, to] pairs.
    templates.statements.time_series_select = 'SELECT TO_TIMESTAMP(dates.f, \'YYYY-MM-DD"T"HH24:MI:SS.FF3\') AS "date_from",\n' +
      'TO_TIMESTAMP(dates.t, \'YYYY-MM-DD"T"HH24:MI:SS.FF3\') AS "date_to" \n' +
      'FROM (\n' +
      '{% for time_item in seria %}' +
      'SELECT \'{{ time_item[0] }}\' f, \'{{ time_item[1] }}\' t FROM DUAL' +
      '{% if not loop.last %} UNION ALL\n{% endif %}' +
      '{% endfor %}' +
      ') dates';

    templates.expressions.like = '{{ expr }} {% if negated %}NOT {% endif %}LIKE {{ pattern }}{% if default_escape %} ESCAPE \'\\\'{% endif %}';
    delete templates.expressions.ilike;
    templates.tesseract.ilike = 'LOWER({{ expr }}) {% if negated %}NOT {% endif %}LIKE LOWER({{ pattern }}){% if default_escape %} ESCAPE \'\\\'{% endif %}';

    // Oracle has no `STRING` type (used by the default in CAST(... AS STRING),
    // e.g. the multi-column count() concatenation). CAST to VARCHAR2 requires a
    // length, so use the max standard VARCHAR2 size.
    templates.types.string = 'VARCHAR2(4000)';

    return templates;
  }

  public newFilter(filter) {
    return new OracleFilter(this, filter);
  }

  public unixTimestampSql() {
    // eslint-disable-next-line quotes
    return `((cast (systimestamp at time zone 'UTC' as date) - date '1970-01-01') * 86400)`;
  }

  public preAggregationTableName(cube, preAggregationName, skipSchema) {
    const name = super.preAggregationTableName(cube, preAggregationName, skipSchema);
    if (name.length > 128) {
      throw new UserError(`Oracle can not work with table names that longer than 64 symbols. Consider using the 'sqlAlias' attribute in your cube and in your pre-aggregation definition for ${name}.`);
    }
    return name;
  }
}
