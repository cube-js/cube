import { PostgresQuery } from './PostgresQuery';
import { UserError } from '../compiler/UserError';

export class CrateQuery extends PostgresQuery {
  public hllInit(_sql): string {
    throw new UserError('Not implemented yet');
  }

  public hllMerge(_sql): string {
    throw new UserError('Not implemented yet');
  }

  public countDistinctApprox(sql: string): string {
    return `hyperloglog_distinct(${sql})`;
  }

  public sqlTemplates() {
    const templates = super.sqlTemplates();
    delete templates.functions.WIDTH_BUCKET;
    return templates;
  }
}
