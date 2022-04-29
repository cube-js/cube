import { BaseQuery } from './BaseQuery';
import { ParamAllocator } from './ParamAllocator';
import { PostgresParamAllocator, GRANULARITY_TO_INTERVAL } from './PostgresQuery';
import { UserError } from '../compiler/UserError';

export class CrateQuery extends BaseQuery {
  newParamAllocator() {
    return new PostgresParamAllocator();
  }

  convertTz(field) {
    return `${field}`;
    // just return the field while debugging
    //return `(${field}::timestamp AT TIME ZONE '${this.timezone}')`;
  }

  timeGroupedColumn(granularity, dimension) {
    return `date_trunc('${GRANULARITY_TO_INTERVAL[granularity]}', ${dimension})`;
  }

  countDistinctApprox(sql) {
    return `hyperloglog_distinct(${sql})`;
  }

  preAggregationTableName(cube, preAggregationName, skipSchema) {
    const name = super.preAggregationTableName(cube, preAggregationName, skipSchema);
    if (name.length > 64) {
      throw new UserError(`Postgres can not work with table names that longer than 64 symbols. Consider using the 'sqlAlias' attribute in your cube and in your pre-aggregation definition for ${name}.`);
    }
    return name;
  }
}
