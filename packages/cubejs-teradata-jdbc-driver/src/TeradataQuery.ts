import { BaseFilter, BaseQuery } from '@cubejs-backend/schema-compiler';
import R, { concat } from 'ramda';

const GRANULARITY_TO_INTERVAL: Record<string, string> = {
  day: 'DD',
  hour: 'HH',
  minute: 'MI',
  second: 'second',
  month: 'MONTH',
  year: 'YEAR'
};

class TeradataFilter extends BaseFilter {
  public likeIgnoreCase(column: any, not: any, param: any, type: string) {
    const p = (!type || type === 'contains' || type === 'ends') ? '%' : '';
    const s = (!type || type === 'contains' || type === 'starts') ? '%' : '';
    return `${column}${not ? ' NOT' : ''} LIKE CONCAT('${p}', ${this.allocateParam(param)}, '${s}')`;
  }
}

export class TeradataQuery extends BaseQuery {
  public newFilter(filter: any) {
    return new TeradataFilter(this, filter);
  }

  public concatStringsSql(strings: string[]) {
    return `CONCAT(${strings.join(', ')})`;
  }

  public convertTz(field: string) {
    return `${field}`;
  }

  public timeStampCast(value: string) {
    return `CAST(OREPLACE(OREPLACE(${value}, 'T', ' '), 'Z', '') AS TIMESTAMP(3))`;
  }

  public dateTimeCast(value: string) {
    return `CAST(OREPLACE(OREPLACE(${value}, 'T', ' '), 'Z', '') AS TIMESTAMP(3))`;
  }

  public subtractInterval(date: string, interval: string) {
    const [number, type] = this.parseInterval(interval);
    console.log(`(${date} - INTERVAL '${number}' ${type})`);
    return `(${date} - INTERVAL '${number}' ${type})`;
  }

  public addInterval(date: string, interval: string) {
    const [number, type] = this.parseInterval(interval);
    console.log(`(${date} + INTERVAL '${number}' ${type})`);
    return `(${date} + INTERVAL '${number}' ${type})`;
  }

  public timeGroupedColumn(granularity: string, dimension: string): string {
    console.log(`TRUNC(CAST(${dimension} AS TIMESTAMP(6)), '${GRANULARITY_TO_INTERVAL[granularity]}')`);
    return `TRUNC(CAST(${dimension} AS TIMESTAMP(6)), '${GRANULARITY_TO_INTERVAL[granularity]}')`;
  }

  public rowNumberColumn() {
    return ', ROW_NUMBER() OVER (ORDER BY 1) AS RowNum_ ';
  }

  public groupByDimensionLimit() {
    const RADIX = 10;
    let startWindow = 0;
    let endWindow = 1000;
    if (this.offset && parseInt(this.offset, RADIX)) {
      startWindow = this.rowLimit === null ? 0 : (this.rowLimit && parseInt(this.rowLimit, 10) || 0);
      endWindow = startWindow + parseInt(this.offset, RADIX);
    } else {
      endWindow = this.rowLimit === null ? 0 : (this.rowLimit && parseInt(this.rowLimit, 10) || 1000);
    }

    return ` QUALIFY RowNum_ BETWEEN ${startWindow} AND ${endWindow}`;
  }

  public commonQuery() {
    return `
      SELECT
      ${this.baseSelect()}
      ${this.rowNumberColumn()}
      FROM
      ${this.query()}
      ${this.groupByClause()}
      ${this.baseHaving(this.measureFilters)}
      ${this.groupByDimensionLimit()}`;
  }

  public simpleQuery() {
    // eslint-disable-next-line prefer-template
    const inlineWhereConditions: any[] = [];
    const commonQuery = this.rewriteInlineWhere(() => this.commonQuery(), inlineWhereConditions);
    const aliases = this.baseSelect()
      .split(' ')
      .map(word => word.replace(',', ''))
      .filter((word) => word.includes('__'))
      .toString();
    // eslint-disable-next-line prefer-template
    return `
        WITH WINDOW_TABLE AS  (
        ${commonQuery} 
        ${this.baseWhere(this.allFilters.concat(inlineWhereConditions))}`
       + `) SELECT ${aliases} FROM WINDOW_TABLE`
       + this.orderBy();
  }
  
  public escapeColumnName(name: string) {
    return `"${name}"`;
  }

  public renderDimensionCaseLabel(label: any, cubeName: any) {
    if (typeof label === 'object' && label.sql) {
      return this.evaluateSql(cubeName, label.sql, null);
    }
    return `"${label}"`;
  }

  public getFieldIndex(id: string) {
    const dimension = this.dimensionsForSelect().find((d: any) => d.dimension === id);
    if (dimension) {
      return super.getFieldIndex(id);
    }
    return this.escapeColumnName(this.aliasName(id, false));
  }

  public seriesSql(timeDimension: any) {
    const values = timeDimension.timeSeries().map(
      ([from, to]: [string, string]) => `select '${from}' f, '${to}' t`
    ).join(' UNION ALL ');
    return `SELECT ${this.timeStampCast('dates.f')} date_from, ${this.timeStampCast('dates.t')} date_to FROM (${values}) AS dates`;
  }

  public defaultRefreshKeyRenewalThreshold() {
    return 120;
  }

  public castToString(sql: any): string {
    return `CAST(${sql} as VARCHAR(1024))`;
  }
}
