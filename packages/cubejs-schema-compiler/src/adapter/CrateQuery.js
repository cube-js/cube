import { PostgresQuery } from './PostgresQuery';
import { UserError } from '../compiler/UserError';

export class CrateQuery extends PostgresQuery {
  hllInit(sql) {
    throw new UserError('Not implemented yet');
  }

  hllMerge(sql) {
    throw new UserError('Not implemented yet');
  }

  // to implement after merge
  countDistinctApprox(sql) {
    throw new UserError('Not implemented yet');
  }
}
