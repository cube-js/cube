import R from 'ramda';
import { BaseFilter, BaseQuery } from '@cubejs-backend/schema-compiler';

const GRANULARITY_TO_INTERVAL: Record<string, string> = {
  day: 'day',
  week: 'week',
  hour: 'hour',
  minute: 'minute',
  second: 'second',
  month: 'month',
  year: 'year'
};

class DatabricksFilter extends BaseFilter {
  public likeIgnoreCase(column: any, not: any, param: any) {
    return `${column}${not ? ' NOT' : ''} LIKE CONCAT('%', ${this.allocateParam(param)}, '%')`;
  }
}

export class DatabricksQuery extends BaseQuery {
  public newFilter(filter: any) {
    return new DatabricksFilter(this, filter);
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
    const [number, type] = this.parseInterval(interval);

    return `(${date} - INTERVAL '${number}' ${type})`;
  }

  public addInterval(date: string, interval: string) {
    const [number, type] = this.parseInterval(interval);

    return `(${date} + INTERVAL '${number}' ${type})`;
  }

  public timeGroupedColumn(granularity: string, dimension: string): string {
    return `date_trunc('${GRANULARITY_TO_INTERVAL[granularity]}', ${dimension})`;
  }

  public escapeColumnName(name: string) {
    return `\`${name}\``;
  }

  public getFieldIndex(id: string) {
    const dimension = this.dimensionsForSelect().find((d: any) => d.dimension === id);
    if (dimension) {
      return super.getFieldIndex(id);
    }
    return this.escapeColumnName(this.aliasName(id, false));
  }

  public unixTimestampSql() {
    return 'unix_timestamp()';
  }

  public seriesSql(timeDimension: any) {
    const values = timeDimension.timeSeries().map(
      ([from, to]: [string, string]) => `select '${from}' f, '${to}' t`
    ).join(' UNION ALL ');
    return `SELECT ${this.timeStampCast('dates.f')} date_from, ${this.timeStampCast('dates.t')} date_to FROM (${values}) AS dates`;
  }

  public orderHashToString(hash: any) {
    if (!hash || !hash.id) {
      return null;
    }

    const fieldIndex = this.getFieldIndex(hash.id);

    if (fieldIndex === null) {
      return null;
    }

    const dimensionsForSelect = this.dimensionsForSelect();
    const dimensionColumns = R.flatten(
      dimensionsForSelect.map((s: any) => s.selectColumns() && s.aliasName())
    )
      .filter(s => !!s);

    if (dimensionColumns.length) {
      const direction = hash.desc ? 'DESC' : 'ASC';
      return `${fieldIndex} ${direction}`;
    }

    return null;
  }

  public groupByClause(isKeysSubquery = false) {
    const dimensionsForSelect = this.dimensionsForSelect();
    const dimensionColumns = R.flatten(
      dimensionsForSelect.map((s: any) => s.selectColumns() && s.aliasName())
    )
      .filter(s => !!s);

    return dimensionColumns.length ? ` GROUP BY ${dimensionColumns.join(', ')}` : '';
  }

  public defaultRefreshKeyRenewalThreshold() {
    return 120;
  }
}
