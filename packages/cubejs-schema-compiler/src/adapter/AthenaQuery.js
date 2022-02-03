import { BaseQuery } from './BaseQuery';
import { PrestodbQuery } from "./PrestodbQuery";

export class AthenaQuery extends PrestodbQuery {
  // preAggregationLoadSql(cube, preAggregation, _tableName) {
  //   return this.preAggregationSql(cube, preAggregation);
  // }
}
