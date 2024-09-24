import { BaseFilter, BaseQuery, BaseTimeDimension } from '@cubejs-backend/schema-compiler';

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
    const [intervalValue, intervalUnit] = interval.split(' ');
    return `${this.timeStampCast(date)} - fromEpoch${intervalUnit}(${intervalValue})`;
  }

  public addInterval(date: string, interval: string) {
    const [intervalValue, intervalUnit] = interval.split(' ');
    return `${this.timeStampCast(date)} + fromEpoch${intervalUnit}(${intervalValue})`;
  }

  public seriesSql(timeDimension: BaseTimeDimension) {
    const values = timeDimension.timeSeries().map(
      ([from, to]) => `select '${from}' f, '${to}' t`
    ).join(' UNION ALL ');
    return `SELECT ${this.timeStampCast('dates.f')} date_from, ${this.timeStampCast('dates.t')} date_to FROM (${values}) AS dates`;
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

  public hllInit(sql: string) {
    return this.countDistinctApprox(sql); // todo: ensure the correct way to do so in pinot
  }

  public hllMerge(sql: string) {
    return this.countDistinctApprox(sql); // todo: ensure the correct way to do so in pinot
  }

  public countDistinctApprox(sql: string) {
    return `DistinctCountHLLPlus(${sql})`;
  }

  protected limitOffsetClause(limit: string | number, offset: string | number) {
    const limitClause = limit != null ? ` LIMIT ${limit}` : '';
    const offsetClause = offset != null ? ` OFFSET ${offset}` : '';
    return `${offsetClause}${limitClause}`;
  }

  public newTimeDimension(timeDimension: any): BaseTimeDimension {
    return new PinotTimeDimension(this, timeDimension);
  }

  public sqlTemplates() {
    const templates = super.sqlTemplates();
    templates.functions.DATETRUNC = 'DATE_TRUNC({{ args_concat }})';
    templates.statements.select = 'SELECT {{ select_concat | map(attribute=\'aliased\') | join(\', \') }} \n' +
      'FROM (\n  {{ from }}\n) AS {{ from_alias }} \n' +
      '{% if group_by %} GROUP BY {{ group_by }}{% endif %}' +
      '{% if order_by %} ORDER BY {{ order_by | map(attribute=\'expr\') | join(\', \') }}{% endif %}' +
      '{% if offset %}\nOFFSET {{ offset }}{% endif %}' +
      '{% if limit %}\nLIMIT {{ limit }}{% endif %}';
    templates.expressions.extract = 'EXTRACT({{ date_part }} FROM {{ expr }})';
    templates.expressions.timestamp_literal = `fromDateTime('{{ value }}', ${DATE_TIME_FORMAT})`;
    templates.quotes.identifiers = '"';
    delete templates.types.time;
    delete templates.types.interval;
    delete templates.types.binary;
    return templates;
  }
}
