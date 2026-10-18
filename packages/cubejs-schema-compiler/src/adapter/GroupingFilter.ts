import type { BaseQuery } from './BaseQuery';
import { UserError } from '../compiler/UserError';

type Filter = { member?: string; dimension?: string; and?: Filter[]; or?: Filter[] };

/** Normalize an opt-in hierarchy rule before joins and rollup matching. */
export class GroupingFilter {
  protected readonly joinedMeasureCubes = new Set<string>();

  public constructor(protected readonly query: BaseQuery) {}

  protected canonical(path: string, measure = false): string {
    const seen = new Set<string>();
    while (true) {
      if (measure && typeof path === 'string' && path.split('.').length > 2) {
        const resolved = this.query.cubeEvaluator.resolveMemberPath(path);
        if (!resolved) {
          throw new UserError(`groupingFilter cannot resolve '${path}'`);
        }
        this.joinedMeasureCubes.add(resolved.targetCube);
        path = resolved.targetPath;
      }
      if (typeof path !== 'string' || path.split('.').length !== 2 || seen.has(path)) {
        throw new UserError('groupingFilter requires simple, acyclic member identities');
      }
      seen.add(path);
      const definition = measure ? this.query.cubeEvaluator.measureByPath(path) : this.query.cubeEvaluator.dimensionByPath(path);
      const alias = 'aliasMember' in definition ? definition.aliasMember : undefined;
      if (typeof alias !== 'string') {
        return path;
      }
      path = alias;
    }
  }

  public filters(): Array<{ member: string; operator: string; values: string[] }> {
    const q = this.query;
    const dependencies = q.collectFrom(
      [...q.measures, ...q.measureFilters], q.collectMemberNamesFor.bind(q),
      'groupingFilterMeasureDependencies', q.queryCache
    );
    const used = new Set(dependencies.filter(p => q.cubeEvaluator.isMeasure(p)).map(p => this.canonical(p, true)));
    const rules = q.cubeEvaluator.groupingFilters.filter(rule => rule.measures.some(m => used.has(this.canonical(m, true))));
    if (!rules.length) {
      return [];
    }
    const requestedMeasures = [...(q.options.measures || []), ...dependencies.filter(p => q.cubeEvaluator.isMeasure(p))];
    requestedMeasures.filter(p => typeof p === 'string').forEach(p => this.canonical(p, true));
    if (rules.some(rule => this.joinedMeasureCubes.has(rule.dimension.split('.')[0]))) {
      throw new UserError('groupingFilter does not support activation through joined cube instances');
    }
    if (!q.useNativeSqlPlanner || q.options.ungrouped || q.options.totalQuery || q.options.subqueryJoins?.length) {
      throw new UserError('groupingFilter requires a supported aggregate Tesseract query');
    }
    if ((q.options.dimensions || []).some(d => typeof d !== 'string')) {
      throw new UserError('groupingFilter does not support grouping sets or expression dimensions');
    }
    const grouped = new Set<string>((q.options.dimensions || []).map(d => this.canonical(d)));
    const explicit = new Set<string>();
    const visit = (filter: Filter) => {
      const member = filter.member || filter.dimension;
      if (member && q.cubeEvaluator.isDimension(member)) {
        explicit.add(this.canonical(member));
      }
      [...(filter.and || []), ...(filter.or || [])].forEach(visit);
    };
    (q.options.filters || []).forEach(visit);
    const additions: Array<{ member: string; operator: string; values: string[] }> = [];

    for (const rule of rules) {
      const target = this.canonical(rule.dimension);
      const levels = rule.levels.map(level => this.canonical(level));
      if (levels.includes(target) || new Set(levels).size !== levels.length) {
        throw new UserError('groupingFilter requires distinct levels and a separate depth dimension');
      }

      // A second default could silently intersect with this rule in the native
      // planner. Reject the conflicting definition until precedence is defined.
      for (const cube of q.cubeEvaluator.cubeNames()) {
        const defaults = q.cubeEvaluator.cubeFromPath(cube).defaultFilters || [];
        if (defaults.some(f => f.memberReference && q.cubeEvaluator.isDimension(f.memberReference) && this.canonical(f.memberReference) === target)) {
          throw new UserError('groupingFilter conflicts with a default filter on the depth dimension');
        }
      }
      if (!explicit.has(target)) {
        let depth = 0;
        levels.forEach((level, index) => {
          if (grouped.has(level)) {
            depth = index;
          }
        });
        additions.push({ member: rule.dimension, operator: 'equals', values: [String(depth)] });
      }
    }
    return additions;
  }
}
