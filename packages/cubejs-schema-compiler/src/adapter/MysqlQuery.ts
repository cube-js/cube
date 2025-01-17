import moment from 'moment-timezone';
import { getEnv, parseSqlInterval } from '@cubejs-backend/shared';
import { BaseQuery } from './BaseQuery';
import { BaseFilter } from './BaseFilter';
import { UserError } from '../compiler/UserError';

const GRANULARITY_TO_INTERVAL = {
  day: (date) => `DATE_FORMAT(${date}, '%Y-%m-%dT00:00:00.000')`,
  week: (date) => `DATE_FORMAT(DATE_ADD('1900-01-01', INTERVAL TIMESTAMPDIFF(WEEK, '1900-01-01', ${date}) WEEK), '%Y-%m-%dT00:00:00.000')`,
  hour: (date) => `DATE_FORMAT(${date}, '%Y-%m-%dT%H:00:00.000')`,
  minute: (date) => `DATE_FORMAT(${date}, '%Y-%m-%dT%H:%i:00.000')`,
  second: (date) => `DATE_FORMAT(${date}, '%Y-%m-%dT%H:%i:%S.000')`,
  month: (date) => `DATE_FORMAT(${date}, '%Y-%m-01T00:00:00.000')`,
  quarter: (date) => `DATE_ADD('1900-01-01', INTERVAL TIMESTAMPDIFF(QUARTER, '1900-01-01', ${date}) QUARTER)`,
  year: (date) => `DATE_FORMAT(${date}, '%Y-01-01T00:00:00.000')`
};

class MysqlFilter extends BaseFilter {
  public likeIgnoreCase(column, not, param, type) {
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
    }

    // No need to support microseconds.

    throw new Error(`Cannot transform interval expression "${interval}" to MySQL dialect`);
  }

  public escapeColumnName(name) {
    return `\`${name}\``;
  }

  public seriesSql(timeDimension) {
    const values = timeDimension.timeSeries().map(
      ([from, to]) => `select '${from}' f, '${to}' t`
    ).join(' UNION ALL ');
    return `SELECT TIMESTAMP(dates.f) date_from, TIMESTAMP(dates.t) date_to FROM (${values}) AS dates`;
  }

  public concatStringsSql(strings) {
    return `CONCAT(${strings.join(', ')})`;
  }

  public unixTimestampSql() {
    return 'UNIX_TIMESTAMP()';
  }

  public wrapSegmentForDimensionSelect(sql) {
    return `IF(${sql}, 1, 0)`;
  }

  public preAggregationTableName(cube, preAggregationName, skipSchema) {
    const name = super.preAggregationTableName(cube, preAggregationName, skipSchema);
    if (name.length > 64) {
      throw new UserError(`MySQL can not work with table names that longer than 64 symbols. Consider using the 'sqlAlias' attribute in your cube and in your pre-aggregation definition for ${name}.`);
    }
    return name;
  }

  public sqlTemplates() {
    const templates = super.sqlTemplates();
    templates.quotes.identifiers = '`';
    templates.quotes.escape = '\\`';
    // NOTE: this template contains a comma; two order expressions are being generated
    templates.expressions.sort = '{{ expr }} IS NULL {% if nulls_first %}DESC{% else %}ASC{% endif %}, {{ expr }} {% if asc %}ASC{% else %}DESC{% endif %}';
    delete templates.expressions.ilike;
    templates.types.string = 'VARCHAR';
    templates.types.boolean = 'TINYINT';
    templates.types.timestamp = 'DATETIME';
    delete templates.types.interval;
    templates.types.binary = 'BLOB';
    return templates;
  }
}
