import R from 'ramda';
import { splitSqlInterval } from '@cubejs-backend/shared';

import { BaseQuery } from './BaseQuery';
import { BaseFilter } from './BaseFilter';
import { BaseTimeDimension } from './BaseTimeDimension';

const GRANULARITY_TO_INTERVAL: Record<string, (date: string) => string> = {
  day: (date: string) => `DATE_FORMAT(${date}, 'yyyy-MM-dd 00:00:00.000')`,
  week: (date: string) => `DATE_FORMAT(from_unixtime(unix_timestamp('1900-01-01 00:00:00') + floor((unix_timestamp(${date}) - unix_timestamp('1900-01-01 00:00:00')) / (60 * 60 * 24 * 7)) * (60 * 60 * 24 * 7)), 'yyyy-MM-dd 00:00:00.000')`,
  hour: (date: string) => `DATE_FORMAT(${date}, 'yyyy-MM-dd HH:00:00.000')`,
  minute: (date: string) => `DATE_FORMAT(${date}, 'yyyy-MM-dd HH:mm:00.000')`,
  second: (date: string) => `DATE_FORMAT(${date}, 'yyyy-MM-dd HH:mm:ss.000')`,
  month: (date: string) => `DATE_FORMAT(${date}, 'yyyy-MM-01 00:00:00.000')`,
  year: (date: string) => `DATE_FORMAT(${date}, 'yyyy-01-01 00:00:00.000')`
};

class HiveFilter extends BaseFilter {
  public likeIgnoreCase(column: string, not: boolean, param: unknown, type: string) {
    const p = (!type || type === 'contains' || type === 'ends') ? '%' : '';
    const s = (!type || type === 'contains' || type === 'starts') ? '%' : '';
    return `${column}${not ? ' NOT' : ''} LIKE CONCAT('${p}', ${this.allocateParam(param)}, '${s}')`;
  }
}

export class HiveQuery extends BaseQuery {
  public newFilter(filter: any) {
    return new HiveFilter(this as BaseQuery, filter);
  }

  public convertTz(field: string) {
    return `from_utc_timestamp(${field}, '${this.timezone}')`;
  }

  public timeStampCast(value: string) {
    return `from_utc_timestamp(replace(replace(${value}, 'T', ' '), 'Z', ''), 'UTC')`;
  }

  public dateTimeCast(value: string) {
    return `from_utc_timestamp(${value}, 'UTC')`; // TODO
  }

  public subtractInterval(date: string, interval: string) {
    return this.applyInterval('-', date, interval);
  }

  public addInterval(date: string, interval: string) {
    return this.applyInterval('+', date, interval);
  }

  /**
   * Hive INTERVAL literals carry a single unit, so a compound interval is applied one unit at a
   * time, coarsest first.
   */
  private applyInterval(operator: '+' | '-', date: string, interval: string): string {
    return splitSqlInterval(interval).reduce((acc, part) => {
      const [number, type] = this.parseInterval(part);

      return `(${acc} ${operator} INTERVAL '${number}' ${type})`;
    }, date);
  }

  public timeGroupedColumn(granularity: string, dimension: string) {
    return GRANULARITY_TO_INTERVAL[granularity](dimension);
  }

  public escapeColumnName(name: string) {
    return `\`${name}\``;
  }

  public simpleQuery() {
    const ungrouped = this.evaluateSymbolSqlWithContext(
      () => `${this.commonQuery()} ${this.baseWhere(this.allFilters)}`, {
        ungroupedForWrappingGroupBy: true
      }
    );
    const select = this.evaluateSymbolSqlWithContext(
      () => this.dimensionsForSelect().map(
        d => d.aliasName()
      ).concat(this.measures.flatMap(m => m.selectColumns())).filter(s => !!s), {
        ungroupedAliases: R.fromPairs(this.forSelect().map((m: any) => [m.measure || m.dimension, m.aliasName()]))
      }
    );
    const query = `SELECT ${select} FROM (${ungrouped}) AS ${this.escapeColumnName('hive_wrapper')}
    ${this.groupByClause()}`;
    return this.baseHaving(query, this.measureFilters) + this.orderBy() + this.groupByDimensionLimit();
  }

  public seriesSql(timeDimension: BaseTimeDimension) {
    const values = timeDimension.timeSeries().map(
      ([from, to]) => `select '${from}' f, '${to}' t`
    ).join(' UNION ALL ');
    return `SELECT ${this.timeStampCast('dates.f')} date_from, ${this.timeStampCast('dates.t')} date_to FROM (${values}) AS dates`;
  }

  public groupByClause() {
    const dimensionsForSelect = this.dimensionsForSelect();
    const dimensionColumns =
      R.flatten(dimensionsForSelect.map(
        s => s.selectColumns() && s.aliasName()
      )).filter(s => !!s);
    return dimensionColumns.length ? ` GROUP BY ${dimensionColumns.join(', ')}` : '';
  }

  public getFieldIndex(id: string) {
    const idx = super.getFieldIndex(id);

    if (idx !== null) {
      return idx;
    }

    return this.escapeColumnName(this.aliasName(id));
  }

  public unixTimestampSql() {
    return 'unix_timestamp()';
  }

  public defaultRefreshKeyRenewalThreshold() {
    return 120;
  }

  public sqlTemplates() {
    const templates = super.sqlTemplates();
    delete templates.functions.WIDTH_BUCKET;
    return templates;
  }
}
