import { BaseQuery } from './BaseQuery';
import { ParamAllocator } from './ParamAllocator';

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

class PostgresParamAllocator extends ParamAllocator {
  public paramPlaceHolder(paramIndex) {
    return `$${paramIndex + 1}`;
  }
}

export class PostgresQuery extends BaseQuery {
  public newParamAllocator(expressionParams) {
    return new PostgresParamAllocator(expressionParams);
  }

  public convertTz(field: string): string {
    return `(${field}::timestamptz AT TIME ZONE '${this.timezone}')`;
  }

  public timeGroupedColumn(granularity: string, dimension: string): string {
    return `date_trunc('${GRANULARITY_TO_INTERVAL[granularity]}', ${dimension})`;
  }

  public dimensionTimeGroupedColumn(dimension: string, interval: string, offset: string): string {
    if (this.isGranularityNaturalAligned(interval)) {
      return super.dimensionTimeGroupedColumn(dimension, interval, offset);
    }

    // Formula:
    // SELECT ((DATE_TRUNC('year', dimension) + offset?) +
    //        FLOOR(
    //          EXTRACT(EPOCH FROM (dimension - (DATE_TRUNC('year', dimension) + offset?))) /
    //          EXTRACT(EPOCH FROM interval)
    //        ) * interval)
    //
    // Should also work for AWS RedShift

    let dtDate = this.timeGroupedColumn('year', dimension);
    if (offset) {
      dtDate = this.addInterval(dtDate, offset);
    }

    return `${dtDate} + FLOOR(
      EXTRACT(EPOCH FROM (${dimension} - (${dtDate}))) /
      EXTRACT(EPOCH FROM INTERVAL '${interval}')
    ) * INTERVAL '${interval}'`;
  }

  public startOfTheYearTimestampSql() {
    return 'date_trunc(\'year\', CURRENT_TIMESTAMP)';
  }

  public hllInit(sql) {
    return `hll_add_agg(hll_hash_any(${sql}))`;
  }

  public hllMerge(sql) {
    return `round(hll_cardinality(hll_union_agg(${sql})))`;
  }

  public countDistinctApprox(sql) {
    return `round(hll_cardinality(hll_add_agg(hll_hash_any(${sql}))))`;
  }

  public sqlTemplates() {
    const templates = super.sqlTemplates();
    // eslint-disable-next-line no-template-curly-in-string
    templates.params.param = '${{ param_index + 1 }}';
    templates.functions.DATETRUNC = 'DATE_TRUNC({{ args_concat }})';
    templates.functions.CONCAT = 'CONCAT({% for arg in args %}CAST({{arg}} AS TEXT){% if not loop.last %},{% endif %}{% endfor %})';
    templates.functions.DATEPART = 'DATE_PART({{ args_concat }})';
    templates.functions.CURRENTDATE = 'CURRENT_DATE';
    templates.functions.LEAST = 'LEAST({{ args_concat }})';
    templates.functions.GREATEST = 'GREATEST({{ args_concat }})';
    templates.functions.NOW = 'NOW({{ args_concat }})';
    // DATEADD is being rewritten to DATE_ADD
    // templates.functions.DATEADD = '({{ args[2] }} + \'{{ interval }} {{ date_part }}\'::interval)';
    // TODO: is DATEDIFF expr worth documenting?
    templates.functions.DATEDIFF = 'CASE WHEN LOWER(\'{{ date_part }}\') IN (\'year\', \'quarter\', \'month\') THEN (EXTRACT(YEAR FROM AGE(DATE_TRUNC(\'{{ date_part }}\', {{ args[2] }}), DATE_TRUNC(\'{{ date_part }}\', {{ args[1] }}))) * 12 + EXTRACT(MONTH FROM AGE(DATE_TRUNC(\'{{ date_part }}\', {{ args[2] }}), DATE_TRUNC(\'{{ date_part }}\', {{ args[1] }})))) / CASE LOWER(\'{{ date_part }}\') WHEN \'year\' THEN 12 WHEN \'quarter\' THEN 3 WHEN \'month\' THEN 1 END ELSE EXTRACT(EPOCH FROM DATE_TRUNC(\'{{ date_part }}\', {{ args[2] }}) - DATE_TRUNC(\'{{ date_part }}\', {{ args[1] }})) / EXTRACT(EPOCH FROM \'1 {{ date_part }}\'::interval) END::bigint';
    templates.expressions.interval = 'INTERVAL \'{{ interval }}\'';
    templates.expressions.extract = 'EXTRACT({{ date_part }} FROM {{ expr }})';
    templates.expressions.timestamp_literal = 'timestamptz \'{{ value }}\'';
    templates.window_frame_types.groups = 'GROUPS';
    templates.types.string = 'TEXT';
    templates.types.tinyint = 'SMALLINT';
    templates.types.float = 'REAL';
    templates.types.double = 'DOUBLE PRECISION';
    templates.types.binary = 'BYTEA';
    return templates;
  }

  public get shouldReuseParams() {
    return true;
  }
}
