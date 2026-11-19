import { getEnv } from '@cubejs-backend/shared';
import { PrestodbQuery } from './PrestodbQuery';

export class TrinoQuery extends PrestodbQuery {
  // Trino doesn't require odd prestodb manual datetime offset calculations
  // as it uses mature timestamps models
  public override convertTz(field: string) {
    return this.timezone ? `CAST((${this.promoteDateToTimestamp(field)} AT TIME ZONE '${this.timezone}') AS TIMESTAMP)` : field;
  }

  public override sqlTemplates() {
    const templates = super.sqlTemplates();
    // Multi-stage results are combined as UNION ALL + GROUP BY: Trino inlines CTEs, so
    // a keys grid would read each one twice, and its null-safe join is not hashable.
    // Trino types a bare NULL from the other branches; `arbitrary` skips NULLs for any type.
    templates.tesseract.full_key_aggregate_union_null = 'NULL';
    templates.functions.GROUP_ANY = 'arbitrary({{ expr }})';
    return templates;
  }

  protected override unionFullKeyAggregateEnabled(): boolean {
    return getEnv('tesseractTrinoUnionFullKeyAggregate');
  }
}
