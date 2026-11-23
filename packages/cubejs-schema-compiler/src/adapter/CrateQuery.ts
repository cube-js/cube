import { PostgresQuery } from './PostgresQuery';
import { BaseFilter } from './BaseFilter';
import { UserError } from '../compiler/UserError';
import type { QueryFilterTreeItem } from './QueryTypes';

export class CrateQuery extends PostgresQuery {
  public newFilter(filter: QueryFilterTreeItem): BaseFilter {
    return new BaseFilter(this, filter);
  }

  public hllInit(_sql: string): string {
    throw new UserError('Not implemented yet');
  }

  public hllMerge(_sql: string): string {
    throw new UserError('Not implemented yet');
  }

  public countDistinctApprox(sql: string): string {
    return `hyperloglog_distinct(${sql})`;
  }

  public sqlTemplates() {
    const templates = super.sqlTemplates();
    delete templates.functions.WIDTH_BUCKET;
    templates.tesseract.number_param_cast = '{{ expr }}';
    templates.functions.APPROXDISTINCT = 'HYPERLOGLOG_DISTINCT({{ args_concat }})';
    return templates;
  }
}
