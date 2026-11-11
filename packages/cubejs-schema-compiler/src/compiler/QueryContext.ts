import { camelize } from 'inflection';
import { UserError } from './UserError';

export type QueryContextTimeDimension = {
  dimension: string;
  granularity?: string;
  dateRange?: string[];
};

export type QueryContextFilter = {
  member: string;
  operator: string;
  values?: Array<string | null>;
};

/**
 * The query node a condition is evaluated against: the query, or the multi-stage stage
 * computing the measure at its own grain. Members are `cube.member` paths; filters are the
 * leaves constraining the whole node (`or` groups are left out); `measures` are the query's.
 */
export type QueryContextData = {
  dimensions: string[];
  timeDimensions: QueryContextTimeDimension[];
  measures: string[];
  filters: QueryContextFilter[];
  segments: string[];
};

export type QueryContextConditionFn = (...args: Array<unknown>) => unknown;

export const QUERY_CONTEXT_ONLY_IN_CASE = 'QUERY_CONTEXT is only available in measure case.when[].context';

export type QueryContextPredicate = 'has_dimension' | 'has_filter' | 'single_value' | 'has_measure' | 'has_segment';

export type QueryContextMemberKind = 'dimensions' | 'measures' | 'segments';

export const QUERY_CONTEXT_PREDICATES: Record<QueryContextPredicate, { memberKinds: QueryContextMemberKind[] }> = {
  has_dimension: { memberKinds: ['dimensions'] },
  has_filter: { memberKinds: ['dimensions', 'measures'] },
  single_value: { memberKinds: ['dimensions', 'measures'] },
  has_measure: { memberKinds: ['measures'] },
  has_segment: { memberKinds: ['segments'] },
};

// Each predicate by its snake_case name and its camelCase alias
const PREDICATE_BY_NAME: Record<string, QueryContextPredicate> = Object.fromEntries(
  (Object.keys(QUERY_CONTEXT_PREDICATES) as QueryContextPredicate[])
    .flatMap(name => [[name, name], [camelize(name, true), name]])
);

export const isQueryContextPredicate = (name: string): boolean => name in PREDICATE_BY_NAME;

/**
 * `QUERY_CONTEXT` as seen by a measure `case.when[].context` function: predicates over
 * the members the node groups by, filters and selects. Member arguments are the same
 * `CUBE.member` references `sql` uses; they stringify to a `cube.member` path.
 */
export type QueryContextMemberCheck = (predicate: QueryContextPredicate, path: string) => void;

export class QueryContext {
  // `check` rejects a member the predicate cannot take before it is looked up, so a
  /// branch the compile-time run did not reach still fails instead of answering false
  public constructor(
    protected readonly data: QueryContextData,
    protected readonly check?: QueryContextMemberCheck,
  ) {
  }

  public has_dimension(member: unknown): boolean {
    return this.evaluate('has_dimension', QueryContext.memberPath(member));
  }

  public has_filter(member: unknown): boolean {
    return this.evaluate('has_filter', QueryContext.memberPath(member));
  }

  public single_value(member: unknown): boolean {
    return this.evaluate('single_value', QueryContext.memberPath(member));
  }

  public has_measure(member: unknown): boolean {
    return this.evaluate('has_measure', QueryContext.memberPath(member));
  }

  public has_segment(member: unknown): boolean {
    return this.evaluate('has_segment', QueryContext.memberPath(member));
  }

  protected evaluate(predicate: QueryContextPredicate, path: string): boolean {
    this.check?.(predicate, path);
    switch (predicate) {
      case 'has_dimension':
        // A time dimension without granularity only carries a date range and is not in GROUP BY
        return this.data.dimensions.includes(path)
          || this.data.timeDimensions.some(td => td.dimension === path && !!td.granularity);
      case 'has_filter':
        return this.filtersOn(path).length > 0
          || this.data.timeDimensions.some(td => td.dimension === path && !!td.dateRange);
      case 'single_value': {
        const filters = this.filtersOn(path);
        return filters.length === 1 && filters[0].operator === 'equals' && filters[0].values?.length === 1;
      }
      case 'has_measure':
        return this.data.measures.includes(path);
      case 'has_segment':
        return this.data.segments.includes(path);
      default:
        throw new Error(`Unknown QUERY_CONTEXT predicate ${predicate}`);
    }
  }

  protected filtersOn(path: string): QueryContextFilter[] {
    return this.data.filters.filter(f => f.member === path);
  }

  protected static memberPath(member: unknown): string {
    if (member === undefined || member === null) {
      throw new UserError('QUERY_CONTEXT predicates expect a member reference like CUBE.member');
    }
    return String(member);
  }

  /**
   * The object bound to the `QUERY_CONTEXT` parameter: only the predicates are reachable,
   * so a typo fails with a list of what exists instead of "is not a function".
   */
  public static symbol(context: QueryContext): QueryContext {
    return new Proxy(context, {
      get(target, prop) {
        if (typeof prop !== 'string') {
          return undefined;
        }
        if (prop === '_objectWithResolvedProperties') {
          return true;
        }
        const predicate = PREDICATE_BY_NAME[prop];
        if (!predicate) {
          throw new UserError(
            `QUERY_CONTEXT.${prop} is not supported. Available predicates: ${Object.keys(QUERY_CONTEXT_PREDICATES).join(', ')}`
          );
        }
        return (member: unknown) => target[predicate](member);
      },
    });
  }
}

/**
 * Runs a condition without a query: every predicate checks its member and answers `true`,
 * so a single run reaches what `and` chains name; an `or` tail is checked at query time.
 */
export class ValidatingQueryContext extends QueryContext {
  public constructor(check: QueryContextMemberCheck) {
    super({ dimensions: [], timeDimensions: [], measures: [], filters: [], segments: [] }, check);
  }

  protected evaluate(predicate: QueryContextPredicate, path: string): boolean {
    this.check?.(predicate, path);
    return true;
  }
}
