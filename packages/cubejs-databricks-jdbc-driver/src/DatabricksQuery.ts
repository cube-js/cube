import R from 'ramda';
import { BaseFilter, BaseQuery } from '@cubejs-backend/schema-compiler';

const GRANULARITY_TO_INTERVAL: Record<string, string> = {
  day: 'day',
  week: 'week',
  hour: 'hour',
  minute: 'minute',
  second: 'second',
  month: 'month',
  quarter: 'quarter',
  year: 'year'
};

class DatabricksFilter extends BaseFilter {
  public likeIgnoreCase(column: any, not: any, param: any, type: string) {
    const p = (!type || type === 'contains' || type === 'ends') ? '%' : '';
    const s = (!type || type === 'contains' || type === 'starts') ? '%' : '';
    return `LOWER(${column})${not ? ' NOT' : ''} LIKE CONCAT('${p}', LOWER(${this.allocateParam(param)}), '${s}')`;
  }
}

export class DatabricksQuery extends BaseQuery {
  public newFilter(filter: any): BaseFilter {
    return new DatabricksFilter(this, filter);
  }

  public hllInit(sql: string) {
    return `hll_sketch_agg(${sql})`;
  }

  public hllMerge(sql: string) {
    return `hll_union_agg(${sql})`;
  }

  public hllCardinality(sql: string): string {
    return `hll_sketch_estimate(${sql})`;
  }

  public hllCardinalityMerge(sql: string): string {
    return `hll_sketch_estimate(hll_union_agg(${sql}))`;
  }

  public countDistinctApprox(sql: string) {
    return `approx_count_distinct(${sql})`;
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

  public defaultRefreshKeyRenewalThreshold() {
    return 120;
  }

  public sqlTemplates() {
    const templates = super.sqlTemplates();
    templates.functions.DATETRUNC = 'DATE_TRUNC({{ args_concat }})';
    templates.functions.BTRIM = 'TRIM({% if args[1] is defined %}{{ args[1] }} FROM {% endif %}{{ args[0] }})';
    templates.functions.LTRIM = 'LTRIM({{ args|reverse|join(", ") }})';
    templates.functions.RTRIM = 'RTRIM({{ args|reverse|join(", ") }})';
    templates.functions.DATEDIFF = 'DATEDIFF({{ date_part }}, DATE_TRUNC(\'{{ date_part }}\', {{ args[1] }}), DATE_TRUNC(\'{{ date_part }}\', {{ args[2] }}))';
    templates.expressions.timestamp_literal = 'from_utc_timestamp(\'{{ value }}\', \'UTC\')';
    templates.quotes.identifiers = '`';
    templates.quotes.escape = '``';
    return templates;
  }
}
