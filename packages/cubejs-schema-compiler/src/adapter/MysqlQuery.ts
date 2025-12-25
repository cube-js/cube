import moment from 'moment-timezone';
import { getEnv, parseSqlInterval } from '@cubejs-backend/shared';
import { BaseQuery } from './BaseQuery';
import { BaseFilter } from './BaseFilter';
import { UserError } from '../compiler/UserError';
import { BaseTimeDimension } from './BaseTimeDimension';

const GRANULARITY_TO_INTERVAL = {
  day: (date: string) => `DATE_FORMAT(${date}, '%Y-%m-%dT00:00:00.000')`,
  week: (date: string) => `DATE_FORMAT(DATE_ADD('1900-01-01', INTERVAL TIMESTAMPDIFF(WEEK, '1900-01-01', ${date}) WEEK), '%Y-%m-%dT00:00:00.000')`,
  hour: (date: string) => `DATE_FORMAT(${date}, '%Y-%m-%dT%H:00:00.000')`,
  minute: (date: string) => `DATE_FORMAT(${date}, '%Y-%m-%dT%H:%i:00.000')`,
  second: (date: string) => `DATE_FORMAT(${date}, '%Y-%m-%dT%H:%i:%S.000')`,
  month: (date: string) => `DATE_FORMAT(${date}, '%Y-%m-01T00:00:00.000')`,
  quarter: (date: string) => `DATE_ADD('1900-01-01', INTERVAL TIMESTAMPDIFF(QUARTER, '1900-01-01', ${date}) QUARTER)`,
  year: (date: string) => `DATE_FORMAT(${date}, '%Y-01-01T00:00:00.000')`
};

class MysqlFilter extends BaseFilter {
  public likeIgnoreCase(column: string, not: boolean, param, type: string) {
    const p = (!type || type === 'contains' || type === 'ends') ? '%' : '';
    const s = (!type || type === 'contains' || type === 'starts') ? '%' : '';
    return `${column}${not ? ' NOT' : ''} LIKE CONCAT('${p}', ${this.allocateParam(param)}, '${s}')`;
  }
}

export class MysqlQuery extends BaseQuery {
  private readonly useNamedTimezones: boolean;

  public constructor(compilers: any, options: any) {
    super(compilers, options);

    this.useNamedTimezones = getEnv('mysqlUseNamedTimezones', { dataSource: this.dataSource });
  }

  public newFilter(filter) {
    return new MysqlFilter(this, filter);
  }

  public castToString(sql: string) {
    return `CAST(${sql} as CHAR)`;
  }

  public convertTz(field: string) {
    if (this.useNamedTimezones) {
      return `CONVERT_TZ(${field}, @@session.time_zone, '${this.timezone}')`;
    }
    return `CONVERT_TZ(${field}, @@session.time_zone, '${moment().tz(this.timezone).format('Z')}')`;
  }

  public timeStampCast(value: string) {
    return `TIMESTAMP(convert_tz(${value}, '+00:00', @@session.time_zone))`;
  }

  public timestampFormat() {
    return 'YYYY-MM-DDTHH:mm:ss.SSS';
  }

  public dateTimeCast(value: string) {
    return `TIMESTAMP(${value})`;
  }

  public subtractInterval(date: string, interval: string) {
    return `DATE_SUB(${date}, INTERVAL ${this.formatInterval(interval)})`;
  }

  public addInterval(date: string, interval: string) {
    return `DATE_ADD(${date}, INTERVAL ${this.formatInterval(interval)})`;
  }

  public timeGroupedColumn(granularity: string, dimension) {
    return `CAST(${GRANULARITY_TO_INTERVAL[granularity](dimension)} AS DATETIME)`;
  }

  /**
   * Returns sql for source expression floored to timestamps aligned with
   * intervals relative to origin timestamp point.
   */
  public dateBin(interval: string, source: string, origin: string): string {
    const intervalFormatted = this.formatInterval(interval);
    const timeUnit = this.isIntervalYM(interval) ? 'MONTH' : 'SECOND';

    return `TIMESTAMPADD(${timeUnit},
        FLOOR(
          TIMESTAMPDIFF(${timeUnit}, ${this.dateTimeCast(`'${origin}'`)}, ${source}) /
          TIMESTAMPDIFF(${timeUnit}, '1970-01-01 00:00:00', '1970-01-01 00:00:00' + INTERVAL ${intervalFormatted})
        ) * TIMESTAMPDIFF(${timeUnit}, '1970-01-01 00:00:00', '1970-01-01 00:00:00' + INTERVAL ${intervalFormatted}),
        ${this.dateTimeCast(`'${origin}'`)}
    )`;
  }

  private isIntervalYM(interval: string): boolean {
    return /(year|month|quarter)/i.test(interval);
  }

  /**
   * The input interval with (possible) plural units, like "2 years", "3 months", "4 weeks", "5 days"...
   * will be converted to MYSQL dialect.
   * @see https://dev.mysql.com/doc/refman/8.4/en/expressions.html#temporal-intervals
   */
  private formatInterval(interval: string): string {
    const intervalParsed = parseSqlInterval(interval);
    const intKeys = Object.keys(intervalParsed).length;

    if (intervalParsed.year && intKeys === 1) {
      return `${intervalParsed.year} YEAR`;
    } else if (intervalParsed.year && intervalParsed.month && intKeys === 2) {
      return `'${intervalParsed.year}-${intervalParsed.month}' YEAR_MONTH`;
    } else if (intervalParsed.quarter && intKeys === 1) {
      return `${intervalParsed.quarter} QUARTER`;
    } else if (intervalParsed.month && intKeys === 1) {
      return `${intervalParsed.month} MONTH`;
    } else if (intervalParsed.week && intKeys === 1) {
      return `${intervalParsed.week} WEEK`;
    } else if (intervalParsed.day && intKeys === 1) {
      return `${intervalParsed.day} DAY`;
    } else if (intervalParsed.day && intervalParsed.hour && intKeys === 2) {
      return `'${intervalParsed.day} ${intervalParsed.hour}' DAY_HOUR`;
    } else if (intervalParsed.day && intervalParsed.hour && intervalParsed.minute && intKeys === 3) {
      return `'${intervalParsed.day} ${intervalParsed.hour}:${intervalParsed.minute}' DAY_MINUTE`;
    } else if (intervalParsed.day && intervalParsed.hour && intervalParsed.minute && intervalParsed.second && intKeys === 4) {
      return `'${intervalParsed.day} ${intervalParsed.hour}:${intervalParsed.minute}:${intervalParsed.second}' DAY_SECOND`;
    } else if (intervalParsed.hour && intervalParsed.minute && intKeys === 2) {
      return `'${intervalParsed.hour}:${intervalParsed.minute}' HOUR_MINUTE`;
    } else if (intervalParsed.hour && intervalParsed.minute && intervalParsed.second && intKeys === 3) {
      return `'${intervalParsed.hour}:${intervalParsed.minute}:${intervalParsed.second}' HOUR_SECOND`;
    } else if (intervalParsed.minute && intervalParsed.second && intKeys === 2) {
      return `'${intervalParsed.minute}:${intervalParsed.second}' MINUTE_SECOND`;
    } else if (intervalParsed.hour && intKeys === 1) {
      return `${intervalParsed.hour} HOUR`;
    } else if (intervalParsed.minute && intKeys === 1) {
      return `${intervalParsed.minute} MINUTE`;
    } else if (intervalParsed.second && intKeys === 1) {
      return `${intervalParsed.second} SECOND`;
    } else if (intervalParsed.millisecond && intKeys === 1) {
      // MySQL doesn't support MILLISECOND, use MICROSECOND instead (1ms = 1000μs)
      return `${intervalParsed.millisecond * 1000} MICROSECOND`;
    }

    throw new Error(`Cannot transform interval expression "${interval}" to MySQL dialect`);
  }

  public escapeColumnName(name: string): string {
    return `\`${name}\``;
  }

  public seriesSql(timeDimension: BaseTimeDimension): string {
    const values = timeDimension.timeSeries().map(
      ([from, to]) => `select '${from}' f, '${to}' t`
    ).join(' UNION ALL ');
    return `SELECT TIMESTAMP(dates.f) date_from, TIMESTAMP(dates.t) date_to FROM (${values}) AS dates`;
  }

  public concatStringsSql(strings: string[]): string {
    return `CONCAT(${strings.join(', ')})`;
  }

  public unixTimestampSql(): string {
    return 'UNIX_TIMESTAMP()';
  }

  public wrapSegmentForDimensionSelect(sql: string): string {
    return `IF(${sql}, 1, 0)`;
  }

  public preAggregationTableName(cube: string, preAggregationName: string, skipSchema: boolean): string {
    const name = super.preAggregationTableName(cube, preAggregationName, skipSchema);
    if (name.length > 64) {
      throw new UserError(`MySQL can not work with table names that longer than 64 symbols. Consider using the 'sqlAlias' attribute in your cube and in your pre-aggregation definition for ${name}.`);
    }
    return name;
  }

  public supportGeneratedSeriesForCustomTd(): boolean {
    return true;
  }

  public intervalString(interval: string): string {
    return this.formatInterval(interval);
  }

  public sqlTemplates() {
    const templates = super.sqlTemplates();
    templates.functions.STRING_AGG = 'GROUP_CONCAT({% if distinct %}DISTINCT {% endif %}{{ args[0] }} SEPARATOR {{ args[1] }})';
    // PERCENTILE_CONT works but requires PARTITION BY
    delete templates.functions.PERCENTILECONT;
    templates.quotes.identifiers = '`';
    templates.quotes.escape = '\\`';
    // NOTE: this template contains a comma; two order expressions are being generated
    templates.expressions.sort = '{{ expr }} IS NULL {% if nulls_first %}DESC{% else %}ASC{% endif %}, {{ expr }} {% if asc %}ASC{% else %}DESC{% endif %}';
    delete templates.expressions.ilike;
    templates.types.string = 'CHAR';
    templates.types.boolean = 'TINYINT';
    templates.types.timestamp = 'DATETIME';
    delete templates.types.interval;
    templates.types.binary = 'BLOB';

    templates.expressions.concat_strings = 'CONCAT({{ strings | join(\',\' ) }})';

    templates.filters.like_pattern = 'CONCAT({% if start_wild %}\'%\'{% else %}\'\'{% endif %}, LOWER({{ value }}), {% if end_wild %}\'%\'{% else %}\'\'{% endif %})';
    templates.tesseract.ilike = 'LOWER({{ expr }}) {% if negated %}NOT {% endif %}LIKE {{ pattern }}';

    templates.statements.time_series_select = 'SELECT TIMESTAMP(dates.f) date_from, TIMESTAMP(dates.t) date_to \n' +
      'FROM (\n' +
      '{% for time_item in seria  %}' +
      '    select \'{{ time_item[0] }}\' f, \'{{ time_item[1] }}\' t \n' +
      '{% if not loop.last %} UNION ALL\n{% endif %}' +
      '{% endfor %}' +
      ') AS dates';

    templates.statements.generated_time_series_select =
      'WITH RECURSIVE date_series AS (\n' +
      '  SELECT TIMESTAMP({{ start }}) AS date_from\n' +
      '  UNION ALL\n' +
      '  SELECT DATE_ADD(date_from, INTERVAL {{ granularity }})\n' +
      '  FROM date_series\n' +
      '  WHERE DATE_ADD(date_from, INTERVAL {{ granularity }}) <= TIMESTAMP({{ end }})\n' +
      ')\n' +
      'SELECT CAST(date_from AS DATETIME) AS date_from,\n' +
      '       CAST(DATE_SUB(DATE_ADD(date_from, INTERVAL {{ granularity }}), INTERVAL 1000 MICROSECOND) AS DATETIME) AS date_to\n' +
      'FROM date_series';

    templates.statements.generated_time_series_with_cte_range_source =
      'WITH RECURSIVE date_series AS (\n' +
      '  SELECT {{ range_source }}.{{ min_name }} AS date_from,\n' +
      '         {{ range_source }}.{{ max_name }} AS max_date\n' +
      '  FROM {{ range_source }}\n' +
      '  UNION ALL\n' +
      '  SELECT DATE_ADD(date_from, INTERVAL {{ granularity }}), max_date\n' +
      '  FROM date_series\n' +
      '  WHERE DATE_ADD(date_from, INTERVAL {{ granularity }}) <= max_date\n' +
      ')\n' +
      'SELECT CAST(date_from AS DATETIME) AS date_from,\n' +
      '       CAST(DATE_SUB(DATE_ADD(date_from, INTERVAL {{ granularity }}), INTERVAL 1000 MICROSECOND) AS DATETIME) AS date_to\n' +
      'FROM date_series';

    return templates;
  }
}
