// eslint-disable-next-line import/no-extraneous-dependencies
import { ParamAllocator, UserError, BaseQuery } from '@cubejs-backend/schema-compiler';

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

class CockroachParamAllocator extends ParamAllocator {
  public paramPlaceHolder(paramIndex: number) {
    return `$${paramIndex + 1}`;
  }
}

export class CockroachQuery extends BaseQuery {
  public newParamAllocator() {
    return new CockroachParamAllocator();
  }

  public convertTz(field: any) {
    return `(${field}::timestamptz AT TIME ZONE '${this.timezone}')`;
  }

  public timeGroupedColumn(granularity: string, dimension: string) {
    return `date_trunc('${GRANULARITY_TO_INTERVAL[granularity]}', ${dimension})`;
  }

  public hllInit(sql: string) {
    return `hll_add_agg(hll_hash_any(${sql}))`;
  }

  public hllMerge(sql: string) {
    return `round(hll_cardinality(hll_union_agg(${sql})))`;
  }

  public countDistinctApprox(sql: string) {
    return `round(hll_cardinality(hll_add_agg(hll_hash_any(${sql}))))`;
  }

  public preAggregationTableName(cube: any, preAggregationName: any, skipSchema: boolean) {
    const name = super.preAggregationTableName(cube, preAggregationName, skipSchema);
    if (name.length > 64) {
      throw new UserError(`Cockroach can not work with table names that longer than 64 symbols. Consider using the 'sqlAlias' attribute in your cube and in your pre-aggregation definition for ${name}.`);
    }
    return name;
  }
}
