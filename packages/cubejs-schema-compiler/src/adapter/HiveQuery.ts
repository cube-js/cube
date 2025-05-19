import R from 'ramda';

import { BaseQuery } from './BaseQuery';
import { BaseFilter } from './BaseFilter';

const GRANULARITY_TO_INTERVAL = {
  day: (date) => `DATE_FORMAT(${date}, 'yyyy-MM-dd 00:00:00.000')`,
  week: (date) => `DATE_FORMAT(from_unixtime(unix_timestamp('1900-01-01 00:00:00') + floor((unix_timestamp(${date}) - unix_timestamp('1900-01-01 00:00:00')) / (60 * 60 * 24 * 7)) * (60 * 60 * 24 * 7)), 'yyyy-MM-dd 00:00:00.000')`,
  hour: (date) => `DATE_FORMAT(${date}, 'yyyy-MM-dd HH:00:00.000')`,
  minute: (date) => `DATE_FORMAT(${date}, 'yyyy-MM-dd HH:mm:00.000')`,
  second: (date) => `DATE_FORMAT(${date}, 'yyyy-MM-dd HH:mm:ss.000')`,
  month: (date) => `DATE_FORMAT(${date}, 'yyyy-MM-01 00:00:00.000')`,
  year: (date) => `DATE_FORMAT(${date}, 'yyyy-01-01 00:00:00.000')`
};

class HiveFilter extends BaseFilter {
  public likeIgnoreCase(column, not, param, type) {
    const p = (!type || type === 'contains' || type === 'ends') ? '%' : '';
    const s = (!type || type === 'contains' || type === 'starts') ? '%' : '';
    return `${column}${not ? ' NOT' : ''} LIKE CONCAT('${p}', ${this.allocateParam(param)}, '${s}')`;
  }
}

export class HiveQuery extends BaseQuery {
  public newFilter(filter) {
    return new HiveFilter(this as BaseQuery, filter);
  }

  public convertTz(field) {
    return `from_utc_timestamp(${field}, '${this.timezone}')`;
  }

  public timeStampCast(value) {
    return `from_utc_timestamp(replace(replace(${value}, 'T', ' '), 'Z', ''), 'UTC')`;
  }

  public dateTimeCast(value) {
    return `from_utc_timestamp(${value}, 'UTC')`; // TODO
  }

  public subtractInterval(date, interval) {
    const [number, type] = this.parseInterval(interval);

    return `(${date} - INTERVAL '${number}' ${type})`;
  }

  public addInterval(date, interval) {
    const [number, type] = this.parseInterval(interval);

    return `(${date} + INTERVAL '${number}' ${type})`;
  }

  public timeGroupedColumn(granularity, dimension) {
    return GRANULARITY_TO_INTERVAL[granularity](dimension);
  }

  public escapeColumnName(name) {
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
    return `SELECT ${select} FROM (${ungrouped}) AS ${this.escapeColumnName('hive_wrapper')}
    ${this.groupByClause()}${this.baseHaving(this.measureFilters)}${this.orderBy()}${this.groupByDimensionLimit()}`;
  }

  public seriesSql(timeDimension) {
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

  public getFieldIndex(id) {
    const dimension = this.dimensionsForSelect().find(d => d.dimension === id);
    if (dimension) {
      return super.getFieldIndex(id);
    }
    return this.escapeColumnName(this.aliasName(id));
  }

  public unixTimestampSql() {
    return 'unix_timestamp()';
  }

  public defaultRefreshKeyRenewalThreshold() {
    return 120;
  }
}
