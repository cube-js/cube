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

  public dimensionTimeGroupedColumn(dimension: string, interval: string, offset: string): string {
    if (offset) {
      offset = this.formatInterval(offset);
    }

    if (this.isGranularityNaturalAligned(interval)) {
      return super.dimensionTimeGroupedColumn(dimension, interval, offset);
    }

    // Formula:
    // SELECT DATEADD(second,
    //         FLOOR(
    //           DATEDIFF(seconds, DATE_TRUNC('year', dimension) + offset?, dimension) /
    //           DATE_PART(epoch_seconds FROM (TIMESTAMP_FROM_PARTS(1970, 1, 1, 0, 0, 0) + interval))
    //         ) * DATE_PART(epoch_seconds FROM (TIMESTAMP_FROM_PARTS(1970, 1, 1, 0, 0, 0) + interval)),
    //         DATE_TRUNC('year', dimension) + offset?)
    //
    // The formula operates with seconds so it won't produce dates aligned with offset date parts, like:
    // if offset is "6 months 3 days" - the result won't always be the 3rd of July. It will add
    // exact number of seconds in the "6 months 3 days" without aligning with natural calendar.

    let dtDate = this.timeGroupedColumn('year', dimension);
    if (offset) {
      dtDate = this.addInterval(dtDate, offset);
    }

    interval = this.formatInterval(interval);

    return `DATEADD(second,
        FLOOR(
          DATEDIFF(seconds, ${dtDate}, CURRENT_TIMESTAMP) /
          DATE_PART(epoch_seconds FROM (TIMESTAMP_FROM_PARTS(1970, 1, 1, 0, 0, 0) + interval '${interval}'))
        ) * DATE_PART(epoch_seconds FROM (TIMESTAMP_FROM_PARTS(1970, 1, 1, 0, 0, 0) + interval '${interval}')),
        ${dtDate})`;
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
    templates.functions.LOG = 'LOG({% if args[1] is undefined %}10, {% endif %}{{ args_concat }})';
    templates.functions.DLOG10 = 'LOG(10, {{ args_concat }})';
    templates.functions.CHARACTERLENGTH = 'LENGTH({{ args[0] }})';
    templates.functions.BTRIM = 'TRIM({{ args_concat }})';
    templates.expressions.extract = 'EXTRACT({{ date_part }} FROM {{ expr }})';
    templates.expressions.interval = 'INTERVAL \'{{ interval }}\'';
    templates.expressions.timestamp_literal = '\'{{ value }}\'::timestamp_tz';
    delete templates.types.interval;
    return templates;
  }
}
