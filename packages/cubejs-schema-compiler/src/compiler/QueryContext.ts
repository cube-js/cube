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
 * The query a measure `case.when[].context` condition is evaluated against.
 * Members are full `cube.member` paths; filters are the leaves of the filter tree,
 * except a time dimension's `dateRange`, which stays on the time dimension.
 */
export type QueryContextData = {
  dimensions: string[];
  timeDimensions: QueryContextTimeDimension[];
  measures: string[];
  filters: QueryContextFilter[];
};

export type QueryContextPredicate = 'grouped' | 'filtered' | 'single_value' | 'has_measure';

export type QueryContextMemberKind = 'dimensions' | 'measures';

export const QUERY_CONTEXT_PREDICATES: Record<QueryContextPredicate, { memberKinds: QueryContextMemberKind[] }> = {
  grouped: { memberKinds: ['dimensions'] },
  filtered: { memberKinds: ['dimensions', 'measures'] },
  single_value: { memberKinds: ['dimensions', 'measures'] },
  has_measure: { memberKinds: ['measures'] },
};

const PREDICATE_BY_NAME: Record<string, QueryContextPredicate> = {
  grouped: 'grouped',
  filtered: 'filtered',
  single_value: 'single_value',
  singleValue: 'single_value',
  has_measure: 'has_measure',
  hasMeasure: 'has_measure',
};

/**
 * `QUERY_CONTEXT` as seen by a measure `case.when[].context` function: predicates over
 * the members the query groups by, filters and selects. Member arguments are the same
 * `CUBE.member` references `sql` uses; they stringify to a `cube.member` path.
 */
export class QueryContext {
  public constructor(protected readonly data: QueryContextData) {
  }

  public grouped(member: unknown): boolean {
    return this.evaluate('grouped', QueryContext.memberPath(member));
  }

  public filtered(member: unknown): boolean {
    return this.evaluate('filtered', QueryContext.memberPath(member));
  }

  public single_value(member: unknown): boolean {
    return this.evaluate('single_value', QueryContext.memberPath(member));
  }

  public has_measure(member: unknown): boolean {
    return this.evaluate('has_measure', QueryContext.memberPath(member));
  }

  protected evaluate(predicate: QueryContextPredicate, path: string): boolean {
    switch (predicate) {
      case 'grouped':
        // A time dimension without granularity only carries a date range and is not in GROUP BY
        return this.data.dimensions.includes(path)
          || this.data.timeDimensions.some(td => td.dimension === path && !!td.granularity);
      case 'filtered':
        return this.filtersOn(path).length > 0
          || this.data.timeDimensions.some(td => td.dimension === path && !!td.dateRange);
      case 'single_value': {
        const filters = this.filtersOn(path);
        return filters.length === 1 && filters[0].operator === 'equals' && filters[0].values?.length === 1;
      }
      case 'has_measure':
        return this.data.measures.includes(path);
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
 * Runs a condition without a query: every predicate reports its member to `check` and
 * returns `result`, so a compile-time dry run can verify the references on both sides of
 * a short-circuiting `&&` / `||` by running once with `true` and once with `false`.
 */
export class ValidatingQueryContext extends QueryContext {
  public constructor(
    protected readonly check: (predicate: QueryContextPredicate, path: string) => void,
    protected readonly result: boolean,
  ) {
    super({ dimensions: [], timeDimensions: [], measures: [], filters: [] });
  }

  protected evaluate(predicate: QueryContextPredicate, path: string): boolean {
    this.check(predicate, path);
    return this.result;
  }
}
