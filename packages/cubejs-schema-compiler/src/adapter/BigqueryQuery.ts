import { parseSqlInterval } from '@cubejs-backend/shared';
import { BaseQuery } from './BaseQuery';
import { BaseFilter } from './BaseFilter';
import { BaseTimeDimension } from './BaseTimeDimension';

const GRANULARITY_TO_INTERVAL = {
  day: 'DAY',
  week: 'WEEK(MONDAY)',
  hour: 'HOUR',
  minute: 'MINUTE',
  second: 'SECOND',
  month: 'MONTH',
  quarter: 'QUARTER',
  year: 'YEAR'
};

class BigqueryFilter extends BaseFilter {
  public likeIgnoreCase(column, not, param, type) {
    const p = (!type || type === 'contains' || type === 'ends') ? '%' : '';
    const s = (!type || type === 'contains' || type === 'starts') ? '%' : '';
    return `LOWER(${column})${not ? ' NOT' : ''} LIKE CONCAT('${p}', LOWER(${this.allocateParam(param)}) , '${s}')`;
  }

  public castParameter() {
    if (this.definition().type === 'boolean') {
      return 'CAST(? AS BOOL)';
    } else if (this.measure || this.definition().type === 'number') {
      // TODO here can be measure type of string actually
      return 'CAST(? AS FLOAT64)';
    }
    return '?';
  }

  public castToString(sql) {
    return `CAST(${sql} as STRING)`;
  }
}

export class BigqueryQuery extends BaseQuery {
  public castToString(sql) {
    return `CAST(${sql} as STRING)`;
  }

  public convertTz(field) {
    return `DATETIME(${field}, '${this.timezone}')`;
  }

  public timeStampCast(value) {
    return `TIMESTAMP(${value})`;
  }

  public dateTimeCast(value) {
    return `DATETIME(TIMESTAMP(${value}))`;
  }

  public escapeColumnName(name) {
    return `\`${name}\``;
  }

  public timeGroupedColumn(granularity, dimension) {
    return `DATETIME_TRUNC(${dimension}, ${GRANULARITY_TO_INTERVAL[granularity]})`;
  }

  /**
   * Returns sql for source expression floored to timestamps aligned with
   * intervals relative to origin timestamp point.
   * BigQuery operates with whole intervals as is without measuring them in plain seconds.
   */
  public dateBin(interval: string, source: string, origin: string): string {
    const [intervalFormatted, timeUnit] = this.formatInterval(interval);
    const beginOfTime = this.dateTimeCast('\'1970-01-01T00:00:00\'');

    return `(${this.dateTimeCast(`'${origin}'`)} + INTERVAL ${intervalFormatted} *
      CAST(FLOOR(
        DATETIME_DIFF(${source}, ${this.dateTimeCast(`'${origin}'`)}, ${timeUnit}) /
        DATETIME_DIFF(${beginOfTime} + INTERVAL ${intervalFormatted}, ${beginOfTime}, ${timeUnit})
      ) AS INT64))`;
  }

  /**
   * The input interval with (possible) plural units, like "2 years", "3 months", "4 weeks", "5 days"...
   * will be converted to BigQuery dialect.
   * @see https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#interval_type
   * It returns a tuple of (formatted interval, timeUnit to use in datediff functions)
   */
  private formatInterval(interval: string): [string, string] {
    const intervalParsed = parseSqlInterval(interval);
    const intKeys = Object.keys(intervalParsed).length;

    if (intervalParsed.year && intKeys === 1) {
      return [`${intervalParsed.year} YEAR`, 'YEAR'];
    } else if (intervalParsed.year && intervalParsed.month && intKeys === 2) {
      return [`'${intervalParsed.year}-${intervalParsed.month}' YEAR TO MONTH`, 'MONTH'];
    } else if (intervalParsed.year && intervalParsed.month && intervalParsed.day && intKeys === 3) {
      return [`'${intervalParsed.year}-${intervalParsed.month} ${intervalParsed.day}' YEAR TO DAY`, 'DAY'];
    } else if (intervalParsed.year && intervalParsed.month && intervalParsed.day && intervalParsed.hour && intKeys === 4) {
      return [`'${intervalParsed.year}-${intervalParsed.month} ${intervalParsed.day} ${intervalParsed.hour}' YEAR TO HOUR`, 'HOUR'];
    } else if (intervalParsed.year && intervalParsed.month && intervalParsed.day && intervalParsed.hour && intervalParsed.minute && intKeys === 5) {
      return [`'${intervalParsed.year}-${intervalParsed.month} ${intervalParsed.day} ${intervalParsed.hour}:${intervalParsed.minute}' YEAR TO MINUTE`, 'MINUTE'];
    } else if (intervalParsed.year && intervalParsed.month && intervalParsed.day && intervalParsed.hour && intervalParsed.minute && intervalParsed.second && intKeys === 6) {
      return [`'${intervalParsed.year}-${intervalParsed.month} ${intervalParsed.day} ${intervalParsed.hour}:${intervalParsed.minute}:${intervalParsed.second}' YEAR TO SECOND`, 'SECOND'];
    } else if (intervalParsed.quarter && intKeys === 1) {
      return [`${intervalParsed.quarter} QUARTER`, 'QUARTER'];
    } else if (intervalParsed.month && intKeys === 1) {
      return [`${intervalParsed.month} MONTH`, 'MONTH'];
    } else if (intervalParsed.month && intervalParsed.day && intKeys === 2) {
      return [`'${intervalParsed.month} ${intervalParsed.day}' MONTH TO DAY`, 'DAY'];
    } else if (intervalParsed.month && intervalParsed.day && intervalParsed.hour && intKeys === 3) {
      return [`'${intervalParsed.month} ${intervalParsed.day} ${intervalParsed.hour}' MONTH TO HOUR`, 'HOUR'];
    } else if (intervalParsed.month && intervalParsed.day && intervalParsed.hour && intervalParsed.minute && intKeys === 4) {
      return [`'${intervalParsed.month} ${intervalParsed.day} ${intervalParsed.hour}:${intervalParsed.minute}' MONTH TO MINUTE`, 'MINUTE'];
    } else if (intervalParsed.month && intervalParsed.day && intervalParsed.hour && intervalParsed.minute && intervalParsed.second && intKeys === 5) {
      return [`'${intervalParsed.month} ${intervalParsed.day} ${intervalParsed.hour}:${intervalParsed.minute}:${intervalParsed.second}' MONTH TO SECOND`, 'SECOND'];
    } else if (intervalParsed.week && intKeys === 1) {
      return [`${intervalParsed.week} WEEK`, 'DAY'];
    } else if (intervalParsed.day && intKeys === 1) {
      return [`${intervalParsed.day} DAY`, 'DAY'];
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

    throw new Error(`Cannot transform interval expression "${interval}" to BigQuery dialect`);
  }

  public newFilter(filter) {
    return new BigqueryFilter(this, filter);
  }

  public dateSeriesSql(timeDimension: BaseTimeDimension) {
    return `${timeDimension.dateSeriesAliasName()} AS (${this.seriesSql(timeDimension)})`;
  }

  public seriesSql(timeDimension: BaseTimeDimension) {
    const values = timeDimension.timeSeries().map(
      ([from, to]) => `select '${from}' f, '${to}' t`
    ).join(' UNION ALL ');
    return `SELECT ${this.dateTimeCast('dates.f')} date_from, ${this.dateTimeCast('dates.t')} date_to FROM (${values}) AS dates`;
  }

  public timestampFormat() {
    return 'YYYY-MM-DD[T]HH:mm:ss.SSSSSS[Z]';
  }

  public timestampPrecision(): number {
    return 6;
  }

  public overTimeSeriesSelect(cumulativeMeasures, dateSeriesSql, baseQuery, dateJoinConditionSql, baseQueryAlias) {
    const forSelect = this.overTimeSeriesForSelect(cumulativeMeasures);
    const outerSeriesAlias = this.cubeAlias('outer_series');
    const outerBase = this.cubeAlias('outer_base');
    const timeDimensionAlias = this.timeDimensions.map(d => d.aliasName()).filter(d => !!d)[0];
    const aliasesForSelect = this.timeDimensions.map(d => d.dateSeriesSelectColumn(outerSeriesAlias)).concat(
      this.dimensions.concat(cumulativeMeasures).map(s => s.aliasName())
    ).filter(c => !!c).join(', ');
    const dateSeriesAlias = this.timeDimensions.map(d => `${d.dateSeriesAliasName()}`).filter(c => !!c)[0];
    return `
    WITH ${dateSeriesSql} SELECT ${aliasesForSelect} FROM
    ${dateSeriesAlias} ${outerSeriesAlias}
    LEFT JOIN (
      SELECT ${forSelect} FROM ${dateSeriesAlias}
      INNER JOIN (${baseQuery}) AS ${baseQueryAlias} ON ${dateJoinConditionSql}
      ${this.groupByClause()}
    ) AS ${outerBase} ON ${outerSeriesAlias}.${this.escapeColumnName('date_from')} = ${outerBase}.${timeDimensionAlias}
    `;
  }

  public subtractInterval(date, interval) {
    return `DATETIME_SUB(${date}, INTERVAL ${this.formatInterval(interval)[0]})`;
  }

  public addInterval(date, interval) {
    return `DATETIME_ADD(${date}, INTERVAL ${this.formatInterval(interval)[0]})`;
  }

  public subtractTimestampInterval(date, interval) {
    return `TIMESTAMP_SUB(${date}, INTERVAL ${this.formatInterval(interval)[0]})`;
  }

  public addTimestampInterval(date, interval) {
    return `TIMESTAMP_ADD(${date}, INTERVAL ${this.formatInterval(interval)[0]})`;
  }

  public nowTimestampSql() {
    return 'CURRENT_TIMESTAMP()';
  }

  public unixTimestampSql() {
    return `UNIX_SECONDS(${this.nowTimestampSql()})`;
  }

  // eslint-disable-next-line no-unused-vars
  public preAggregationLoadSql(cube, preAggregation, tableName) {
    return this.preAggregationSql(cube, preAggregation);
  }

  public hllInit(sql) {
    return `HLL_COUNT.INIT(${sql})`;
  }

  public hllMerge(sql) {
    return `HLL_COUNT.MERGE(${sql})`;
  }

  public countDistinctApprox(sql) {
    return `APPROX_COUNT_DISTINCT(${sql})`;
  }

  public concatStringsSql(strings) {
    return `CONCAT(${strings.join(', ')})`;
  }

  public defaultRefreshKeyRenewalThreshold() {
    return 120;
  }

  public defaultEveryRefreshKey() {
    return {
      every: '2 minutes'
    };
  }

  public sqlTemplates() {
    const templates = super.sqlTemplates();
    templates.quotes.identifiers = '`';
    templates.quotes.escape = '\\`';
    templates.functions.DATETRUNC = 'DATETIME_TRUNC(CAST({{ args[1] }} AS DATETIME), {{ date_part }})';
    templates.functions.LOG = 'LOG({{ args_concat }}{% if args[1] is undefined %}, 10{% endif %})';
    templates.functions.BTRIM = 'TRIM({{ args_concat }})';
    templates.functions.STRPOS = 'STRPOS({{ args_concat }})';
    templates.functions.DATEDIFF = 'DATETIME_DIFF(CAST({{ args[2] }} AS DATETIME), CAST({{ args[1] }} AS DATETIME), {{ date_part }})';
    // DATEADD is being rewritten to DATE_ADD
    // templates.functions.DATEADD = 'DATETIME_ADD(CAST({{ args[2] }} AS DATETTIME), INTERVAL {{ interval }} {{ date_part }})';
    templates.functions.CURRENTDATE = 'CURRENT_DATE';
    delete templates.functions.TO_CHAR;
    templates.expressions.binary = '{% if op == \'%\' %}MOD({{ left }}, {{ right }}){% else %}({{ left }} {{ op }} {{ right }}){% endif %}';
    templates.expressions.interval = 'INTERVAL {{ interval }}';
    templates.expressions.extract = 'EXTRACT({% if date_part == \'DOW\' %}DAYOFWEEK{% elif date_part == \'DOY\' %}DAYOFYEAR{% else %}{{ date_part }}{% endif %} FROM {{ expr }})';
    templates.expressions.timestamp_literal = 'TIMESTAMP(\'{{ value }}\')';
    delete templates.expressions.ilike;
    delete templates.expressions.like_escape;
    templates.types.boolean = 'BOOL';
    templates.types.float = 'FLOAT64';
    templates.types.double = 'FLOAT64';
    templates.types.decimal = 'BIGDECIMAL({{ precision }},{{ scale }})';
    templates.types.binary = 'BYTES';
    templates.operators.is_not_distinct_from = 'IS NOT DISTINCT FROM';
    templates.join_types.full = 'FULL';
    return templates;
  }
}
