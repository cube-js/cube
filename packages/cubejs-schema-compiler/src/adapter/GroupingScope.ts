import type { BaseQuery } from './BaseQuery';
import { UserError } from '../compiler/UserError';

type QueryFilter = {
  member?: string;
  dimension?: string;
  operator?: string;
  and?: QueryFilter[];
  or?: QueryFilter[];
  values?: unknown[];
};

/**
 * Query-local grouping context for models that store contributions at several
 * hierarchy depths. Read the original aggregate branch, not an inner stage's
 * grain. The model decides how to use the result; this class adds no filters.
 */
export class GroupingScope {
  protected readonly canonicalDimensions = new Map<string, string>();

  protected readonly results = new Map<string, number | null>();

  protected context?: { grouped: Set<string>; filtered: Set<string> };

  protected measureMembers?: Set<string>;

  protected collectingMeasures = false;

  public constructor(protected readonly query: BaseQuery) {}

  protected canonicalMember(path: string, kind: 'dimension' | 'measure'): string {
    const seen = new Set<string>();
    let current = path;
    while (true) {
      if (typeof current !== 'string' || current.split('.').length !== 2 || seen.has(current)) {
        throw new UserError(`groupingDepth requires simple, acyclic cube.${kind} identities; join paths and expressions are unsupported`);
      }
      seen.add(current);
      const member = kind === 'dimension' ?
        this.query.cubeEvaluator.dimensionByPath(current) : this.query.cubeEvaluator.measureByPath(current);
      const alias = 'aliasMember' in member ? member.aliasMember : undefined;
      if (typeof alias !== 'string') {
        return current;
      }
      current = alias;
    }
  }

  protected dimension(path: string): string {
    let member = this.canonicalDimensions.get(path);
    if (member === undefined) {
      member = this.canonicalMember(path, 'dimension');
      this.canonicalDimensions.set(path, member);
    }
    return member;
  }

  protected initialize(): { grouped: Set<string>; filtered: Set<string> } {
    if (this.context) {
      return this.context;
    }
    const q = this.query;
    if (!q.useNativeSqlPlanner) {
      throw new UserError('groupingDepth requires the Tesseract SQL planner');
    }
    if (q.options.ungrouped || q.options.preAggregationQuery || q.options.totalQuery) {
      throw new UserError('groupingDepth does not support ungrouped, pre-aggregation, or totalQuery modes');
    }
    if ((q.options.dimensions || []).some(d => typeof d !== 'string')) {
      throw new UserError('groupingDepth does not support expression dimensions or grouping sets');
    }
    // A rollup without grouping context must never satisfy a context-dependent
    // query. Keep this conservative until rollup matching carries that context.
    if (q.cubeEvaluator.cubeNames().some(name => Object.keys(q.cubeEvaluator.cubeFromPath(name).preAggregations || {}).length)) {
      throw new UserError('groupingDepth does not support models with pre-aggregations');
    }
    const grouped = new Set<string>((q.options.dimensions || []).map(d => this.dimension(d)));
    const filtered = new Set<string>();
    const visit = (filter: QueryFilter) => {
      const member = filter.member || filter.dimension;
      if (member && q.cubeEvaluator.isDimension(member)) {
        filtered.add(this.dimension(member));
      }

      for (const child of [...(filter.and || []), ...(filter.or || [])]) {
        visit(child);
      }
      if (filter.operator === 'and' || filter.operator === 'or') {
        for (const child of filter.values || []) {
          if (child && typeof child === 'object') {
            visit(child as QueryFilter);
          }
        }
      }
    };

    for (const filter of q.options.filters || []) {
      visit(filter);
    }
    // Assign only after validation so a failed attempt cannot reuse partial state.
    this.context = { grouped, filtered };
    return this.context;
  }

  protected active(measures: string[]): boolean {
    if (!Array.isArray(measures) || !measures.length) {
      throw new UserError('groupingDepth activeMeasures must be a nonempty array');
    }
    const configured = measures.map(member => this.canonicalMember(member, 'measure'));
    if (!this.measureMembers) {
      // Reuse dependency traversal, including derived measures and HAVING. Its
      // structural SQL evaluation may invoke this helper again; break recursion
      // without caching the placeholder as an executable query's answer.
      this.collectingMeasures = true;

      try {
        const q = this.query;
        const paths = q.collectFrom(
          [...q.measures, ...q.measureFilters],
          q.collectMemberNamesFor.bind(q),
          'collectMemberNamesFor',
          q.queryCache
        );
        this.measureMembers = new Set(paths.filter(p => q.cubeEvaluator.isMeasure(p)).map(p => this.canonicalMember(p, 'measure')));
      } finally {
        this.collectingMeasures = false;
      }
    }
    return configured.some(member => this.measureMembers!.has(member));
  }

  public depth(levels: string[], override: string, activeMeasures?: string[]): number | null {
    if (this.collectingMeasures) {
      return 0;
    }
    if (!Array.isArray(levels) || levels.length === 0 || typeof override !== 'string') {
      throw new UserError('groupingDepth requires nonempty ordered levels and an explicit-override dimension');
    }
    const key = JSON.stringify([levels, override, activeMeasures]);
    if (this.results.has(key)) {
      return this.results.get(key)!;
    }
    if (activeMeasures !== undefined && !this.active(activeMeasures)) {
      this.results.set(key, null);
      return null;
    }
    const { grouped, filtered } = this.initialize();
    const canonicalLevels = levels.map(level => this.dimension(level));
    if (new Set(canonicalLevels).size !== canonicalLevels.length) {
      throw new UserError('groupingDepth levels must resolve to distinct dimensions');
    }
    if (canonicalLevels.some(level => this.query.cubeEvaluator.dimensionByPath(level).type === 'time')) {
      throw new UserError('groupingDepth does not support time dimensions as hierarchy levels');
    }
    const target = this.dimension(override);
    if (canonicalLevels.includes(target)) {
      throw new UserError('groupingDepth override must be separate from hierarchy levels');
    }

    for (const name of this.query.cubeEvaluator.cubeNames()) {
      const filters = this.query.cubeEvaluator.cubeFromPath(name).defaultFilters || [];
      if (filters.some(filter => filter.memberReference && this.query.cubeEvaluator.isDimension(filter.memberReference) && this.dimension(filter.memberReference) === target)) {
        throw new UserError('groupingDepth does not support default filters on its override dimension');
      }
    }
    let value: number | null = null;
    if (!filtered.has(target)) {
      value = 0;
      canonicalLevels.forEach((level, index) => {
        if (grouped.has(level)) {
          value = index;
        }
      });
    }
    this.results.set(key, value);
    return value;
  }
}
