import { PostgresQuery } from './PostgresQuery';
import { UserError } from '../compiler/UserError';

export class CrateQuery extends PostgresQuery {
  public hllInit(_sql): string {
    throw new UserError('Not implemented yet');
  }

  public hllMerge(_sql): string {
    throw new UserError('Not implemented yet');
  }

  // to implement after merge
  public countDistinctApprox(_sql): string {
    throw new UserError('Not implemented yet');
  }
}
