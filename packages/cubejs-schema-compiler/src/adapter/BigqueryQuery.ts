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
    return `TIMESTAMP(DATETIME(${field}), '${this.timezone}')`;
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
    return this.timeStampCast(`DATETIME_TRUNC(${dimension}, ${GRANULARITY_TO_INTERVAL[granularity]})`);
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
        DATETIME_DIFF(${this.dateTimeCast(source)}, ${this.dateTimeCast(`'${origin}'`)}, ${timeUnit}) /
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

  public override intervalAndMinimalTimeUnit(interval: string): [string, string] {
    return this.formatInterval(interval);
  }

  public newFilter(filter) {
    return new BigqueryFilter(this, filter);
  }

  public seriesSql(timeDimension: BaseTimeDimension) {
    const values = timeDimension.timeSeries().map(
      ([from, to]) => `select '${from}' f, '${to}' t`
    ).join(' UNION ALL ');
    return `SELECT ${this.timeStampCast('dates.f')} date_from, ${this.timeStampCast('dates.t')} date_to FROM (${values}) AS dates`;
  }

  public timestampFormat() {
    return 'YYYY-MM-DD[T]HH:mm:ss.SSSSSS[Z]';
  }

  public timestampPrecision(): number {
    return 6;
  }

  public subtractInterval(date, interval) {
    const [intervalFormatted, timeUnit] = this.formatInterval(interval);
    if (['YEAR', 'MONTH', 'QUARTER'].includes(timeUnit) || intervalFormatted.includes('WEEK')) {
      return this.timeStampCast(`DATETIME_SUB(DATETIME(${date}), INTERVAL ${intervalFormatted})`);
    }

    return `TIMESTAMP_SUB(${date}, INTERVAL ${intervalFormatted})`;
  }

  public addInterval(date, interval) {
    const [intervalFormatted, timeUnit] = this.formatInterval(interval);
    if (['YEAR', 'MONTH', 'QUARTER'].includes(timeUnit) || intervalFormatted.includes('WEEK')) {
      return this.timeStampCast(`DATETIME_ADD(DATETIME(${date}), INTERVAL ${intervalFormatted})`);
    }

    return `TIMESTAMP_ADD(${date}, INTERVAL ${intervalFormatted})`;
  }

  public subtractTimestampInterval(timestamp, interval) {
    return this.subtractInterval(timestamp, interval);
  }

  public intervalString(interval: string): string {
    return `${interval}`;
  }

  public addTimestampInterval(timestamp, interval) {
    return this.addInterval(timestamp, interval);
  }

  public nowTimestampSql() {
    return 'CURRENT_TIMESTAMP()';
  }

  public unixTimestampSql() {
    return `UNIX_SECONDS(${this.nowTimestampSql()})`;
  }

  /**
   * Should be protected, but BaseQuery is in js
   * Overridden from BaseQuery to support BigQuery strict data types for
   * joining conditions (note timeStampCast)
   */
  public override runningTotalDateJoinCondition() {
    return this.timeDimensions
      .map(
        d => [
          d,
          (_dateFrom: string, dateTo: string, dateField: string, dimensionDateFrom: string, _dimensionDateTo: string) => `${dateField} >= ${dimensionDateFrom} AND ${dateField} <= ${this.timeStampCast(dateTo)}`
        ]
      );
  }

  /**
   * Should be protected, but BaseQuery is in js
   * Overridden from BaseQuery to support BigQuery strict data types for
   * joining conditions (note timeStampCast)
   */
  public override rollingWindowToDateJoinCondition(granularity) {
    return this.timeDimensions
      .filter(td => td.granularity)
      .map(
        d => [
          d,
          (dateFrom: string, dateTo: string, dateField: string, _dimensionDateFrom: string, _dimensionDateTo: string, _isFromStartToEnd: boolean) => `${dateField} >= ${this.timeGroupedColumn(granularity, dateFrom)} AND ${dateField} <= ${this.timeStampCast(dateTo)}`
        ]
      );
  }

  /**
   * Should be protected, but BaseQuery is in js
   * Overridden from BaseQuery to support BigQuery strict data types for
   * joining conditions (note timeStampCast)
   */
  public override rollingWindowDateJoinCondition(trailingInterval, leadingInterval, offset) {
    offset = offset || 'end';
    return this.timeDimensions
      .filter(td => td.granularity)
      .map(
        d => [d, (dateFrom: string, dateTo: string, dateField: string, _dimensionDateFrom: string, _dimensionDateTo: string, isFromStartToEnd: boolean) => {
        // dateFrom based window
          const conditions: string[] = [];
          if (trailingInterval !== 'unbounded') {
            const startDate = isFromStartToEnd || offset === 'start' ? dateFrom : dateTo;
            const trailingStart = trailingInterval ? this.subtractInterval(startDate, trailingInterval) : startDate;
            const sign = offset === 'start' ? '>=' : '>';
            conditions.push(`${dateField} ${sign} ${this.timeStampCast(trailingStart)}`);
          }
          if (leadingInterval !== 'unbounded') {
            const endDate = isFromStartToEnd || offset === 'end' ? dateTo : dateFrom;
            const leadingEnd = leadingInterval ? this.addInterval(endDate, leadingInterval) : endDate;
            const sign = offset === 'end' ? '<=' : '<';
            conditions.push(`${dateField} ${sign} ${this.timeStampCast(leadingEnd)}`);
          }
          return conditions.length ? conditions.join(' AND ') : '1 = 1';
        }]
      );
  }

  // Should be protected, but BaseQuery is in js
  public override dateFromStartToEndConditionSql(dateJoinCondition, fromRollup, isFromStartToEnd) {
    return dateJoinCondition.map(
      ([d, f]) => ({
        filterToWhere: () => {
          const timeSeries = d.timeSeries();
          return f(
            isFromStartToEnd ?
              this.timeStampCast(this.paramAllocator.allocateParam(timeSeries[0][0])) :
              `${this.timeStampInClientTz(d.dateFromParam())}`,
            isFromStartToEnd ?
              this.timeStampCast(this.paramAllocator.allocateParam(timeSeries[timeSeries.length - 1][1])) :
              `${this.timeStampInClientTz(d.dateToParam())}`,
            `${fromRollup ? this.dimensionSql(d) : d.convertedToTz()}`,
            `${this.timeStampInClientTz(d.dateFromParam())}`,
            `${this.timeStampInClientTz(d.dateToParam())}`,
            isFromStartToEnd
          );
        }
      })
    );
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
    templates.functions.DATETRUNC = 'TIMESTAMP(DATETIME_TRUNC(CAST({{ args[1] }} AS DATETIME), {% if date_part|upper == \'WEEK\' %}{{ \'WEEK(MONDAY)\' }}{% else %}{{ date_part }}{% endif %}))';
    templates.functions.LOG = 'LOG({{ args_concat }}{% if args[1] is undefined %}, 10{% endif %})';
    templates.functions.BTRIM = 'TRIM({{ args_concat }})';
    templates.functions.STRPOS = 'STRPOS({{ args_concat }})';
    templates.functions.DATEDIFF = 'DATETIME_DIFF(CAST({{ args[2] }} AS DATETIME), CAST({{ args[1] }} AS DATETIME), {{ date_part }})';
    // DATEADD is being rewritten to DATE_ADD
    templates.functions.DATE_ADD = 'DATETIME_ADD(DATETIME({{ args[0] }}), INTERVAL {{ interval }} {{ date_part }})';
    templates.functions.CURRENTDATE = 'CURRENT_DATE';
    templates.functions.DATE = 'TIMESTAMP({{ args_concat }})';
    delete templates.functions.TO_CHAR;
    delete templates.functions.PERCENTILECONT;
    templates.expressions.binary = '{% if op == \'%\' %}MOD({{ left }}, {{ right }}){% else %}({{ left }} {{ op }} {{ right }}){% endif %}';
    templates.expressions.interval = 'INTERVAL {{ interval }}';
    templates.expressions.extract = 'EXTRACT({% if date_part == \'DOW\' %}DAYOFWEEK{% elif date_part == \'DOY\' %}DAYOFYEAR{% else %}{{ date_part }}{% endif %} FROM {{ expr }})';
    templates.expressions.timestamp_literal = 'TIMESTAMP(\'{{ value }}\')';
    templates.expressions.rolling_window_expr_timestamp_cast = 'TIMESTAMP({{ value }})';
    delete templates.expressions.ilike;
    delete templates.expressions.like_escape;
    templates.filters.like_pattern = 'CONCAT({% if start_wild %}\'%\'{% else %}\'\'{% endif %}, LOWER({{ value }}), {% if end_wild %}\'%\'{% else %}\'\'{% endif %})';
    templates.tesseract.ilike = 'LOWER({{ expr }}) {% if negated %}NOT {% endif %} LIKE {{ pattern }}';
    templates.tesseract.series_bounds_cast = 'TIMESTAMP({{ expr }})';
    templates.tesseract.bool_param_cast = 'CAST({{ expr }} AS BOOL)';
    templates.tesseract.number_param_cast = 'CAST({{ expr }} AS FLOAT64)';
    templates.types.boolean = 'BOOL';
    templates.types.float = 'FLOAT64';
    templates.types.double = 'FLOAT64';
    templates.types.decimal = 'BIGDECIMAL({{ precision }},{{ scale }})';
    templates.types.binary = 'BYTES';
    templates.expressions.cast_to_string = 'CAST({{ expr }} AS STRING)';
    templates.operators.is_not_distinct_from = 'IS NOT DISTINCT FROM';
    templates.join_types.full = 'FULL';
    templates.statements.time_series_select = 'SELECT DATETIME(TIMESTAMP(f)) date_from, DATETIME(TIMESTAMP(t)) date_to \n' +
    'FROM (\n' +
    '{% for time_item in seria  %}' +
    '    select \'{{ time_item[0] }}\' f, \'{{ time_item[1] }}\' t \n' +
    '{% if not loop.last %} UNION ALL\n{% endif %}' +
    '{% endfor %}' +
    ') AS dates';
    templates.statements.generated_time_series_select = 'SELECT DATETIME(d) AS date_from,\n' +
    'DATETIME_SUB(DATETIME_ADD(DATETIME(d),  INTERVAL {{ granularity }}), INTERVAL 1 MILLISECOND) AS date_to \n' +
    'FROM UNNEST(\n' +
    '{% if minimal_time_unit|upper in ["DAY", "WEEK", "MONTH", "QUARTER", "YEAR"] %}' +
    'GENERATE_DATE_ARRAY(DATE({{ start }}), DATE({{ end }}), INTERVAL {{ granularity }})\n' +
    '{% else %}' +
    'GENERATE_TIMESTAMP_ARRAY(TIMESTAMP({{ start }}), TIMESTAMP({{ end }}), INTERVAL {{ granularity }})\n' +
    '{% endif %}' +
    ') AS d';

    templates.statements.generated_time_series_with_cte_range_source = 'SELECT DATETIME(d) AS date_from,\n' +
    'DATETIME_SUB(DATETIME_ADD(DATETIME(d),  INTERVAL {{ granularity }}), INTERVAL 1 MILLISECOND) AS date_to \n' +
    'FROM {{ range_source }}, UNNEST(\n' +
    '{% if minimal_time_unit|upper in ["DAY", "WEEK", "MONTH", "QUARTER", "YEAR"] %}' +
    'GENERATE_DATE_ARRAY(DATE({{ range_source }}.{{ min_name }}), DATE({{ range_source }}.{{ max_name }}), INTERVAL {{ granularity }})\n' +
    '{% else %}' +
    'GENERATE_TIMESTAMP_ARRAY(TIMESTAMP({{ range_source }}.{{ min_name }}), TIMESTAMP({{ range_source }}.{{ max_name }}), INTERVAL {{ granularity }})\n' +
    '{% endif %}' +
    ') AS d';
    return templates;
  }
}
