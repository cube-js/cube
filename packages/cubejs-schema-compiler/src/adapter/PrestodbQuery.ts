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

  public subtractInterval(date, interval) {
    const [intervalValue, intervalUnit] = interval.split(' ');
    return `${date} - interval '${intervalValue}' ${intervalUnit}`;
  }

  public addInterval(date, interval) {
    const [intervalValue, intervalUnit] = interval.split(' ');
    return `${date} + interval '${intervalValue}' ${intervalUnit}`;
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

  protected limitOffsetClause(limit, offset) {
    const limitClause = limit != null ? ` LIMIT ${limit}` : '';
    const offsetClause = offset != null ? ` OFFSET ${offset}` : '';
    return `${offsetClause}${limitClause}`;
  }

  public sqlTemplates() {
    const templates = super.sqlTemplates();
    templates.functions.DATETRUNC = 'DATE_TRUNC({{ args_concat }})';
    templates.functions.DATEPART = 'DATE_PART({{ args_concat }})';
    templates.statements.select = 'SELECT {{ select_concat | map(attribute=\'aliased\') | join(\', \') }} \n' +
      'FROM (\n  {{ from }}\n) AS {{ from_alias }} \n' +
      '{% if group_by %} GROUP BY {{ group_by }}{% endif %}' +
      '{% if order_by %} ORDER BY {{ order_by | map(attribute=\'expr\') | join(\', \') }}{% endif %}' +
      '{% if offset is not none %}\nOFFSET {{ offset }}{% endif %}' +
      '{% if limit is not none %}\nLIMIT {{ limit }}{% endif %}';
    templates.expressions.extract = 'EXTRACT({{ date_part }} FROM {{ expr }})';
    templates.expressions.interval_single_date_part = 'INTERVAL \'{{ num }}\' {{ date_part }}';
    templates.expressions.timestamp_literal = 'from_iso8601_timestamp(\'{{ value }}\')';
    delete templates.expressions.ilike;
    templates.types.string = 'VARCHAR';
    templates.types.float = 'REAL';
    // Presto intervals have a YearMonth or DayTime type variants, but no universal type
    delete templates.types.interval;
    templates.types.binary = 'VARBINARY';
    return templates;
  }

  public castToString(sql: any): string {
    return `CAST(${sql} as VARCHAR)`;
  }
}
