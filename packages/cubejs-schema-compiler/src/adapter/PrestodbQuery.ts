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

  public groupByDimensionLimit() {
    const limitClause = this.rowLimit === null ? '' : ` LIMIT ${this.rowLimit && parseInt(this.rowLimit, 10) || 10000}`;
    const offsetClause = this.offset ? ` OFFSET ${parseInt(this.offset, 10)}` : '';

    return `${offsetClause}${limitClause}`;
  }

  public sqlTemplates() {
    const templates = super.sqlTemplates();
    templates.functions.DATETRUNC = 'DATE_TRUNC({{ args_concat }})';
    templates.functions.DATEPART = 'DATE_PART({{ args_concat }})';
    templates.expressions.extract = 'EXTRACT({{ date_part }} FROM {{ expr }})';
    templates.expressions.interval = 'INTERVAL \'{{ num }}\' {{ date_part }}';
    return templates;
  }
}
