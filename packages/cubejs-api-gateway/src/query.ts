import R from 'ramda';
import moment from 'moment-timezone';
import Joi from 'joi';
import { CacheMode, canonicalTimezone, getEnv } from '@cubejs-backend/shared';

import { UserError } from './user-error';
import { dateParser } from './date-parser';
import { QueryType as QueryTypeEnum } from './types/enums';
import type { QueryType } from './types/strings';
import type { InputMemberExpression, NormalizedQuery, NormalizedQueryFilter, Query } from './types/query';

const getQueryGranularity = (queries: NormalizedQuery[]): string[] => R.pipe(
  R.map(({ timeDimensions }: NormalizedQuery) => timeDimensions![0]?.granularity),
  R.filter(Boolean),
  R.uniq
)(queries) as string[];

const getPivotQuery = (queryType: QueryType, queries: NormalizedQuery[]): any => {
  let pivotQuery: any = queries[0];

  if (queryType === QueryTypeEnum.BLENDING_QUERY) {
    pivotQuery = R.fromPairs(
      ['measures', 'dimensions'].map(
        (key) => [key, R.uniq(queries.reduce((memo: any[], q) => memo.concat(q[key]), []))]
      )
    );

    const [granularity] = getQueryGranularity(queries);

    pivotQuery.timeDimensions = [{
      dimension: 'time',
      granularity
    }];
  } else if (queryType === QueryTypeEnum.COMPARE_DATE_RANGE_QUERY) {
    pivotQuery.dimensions = ['compareDateRange'].concat(pivotQuery.dimensions || []);
  }

  pivotQuery.queryType = queryType;

  return pivotQuery;
};

const parsedPatchMeasureFilterExpression = Joi.array().items(Joi.string());

const evaluatedPatchMeasureFilterExpression = Joi.object().keys({
  sql: Joi.func().required(),
});

const parsedPatchMeasureExpression = Joi.object().keys({
  type: Joi.valid('PatchMeasure').required(),
  sourceMeasure: Joi.string().required(),
  replaceAggregationType: Joi.string().allow(null).required(),
  addFilters: Joi.array().items(parsedPatchMeasureFilterExpression).required(),
});

const evaluatedPatchMeasureExpression = parsedPatchMeasureExpression.keys({
  addFilters: Joi.array().items(evaluatedPatchMeasureFilterExpression).required(),
});

const id = Joi.string().regex(/^[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+$/);

const cacheModeSchema = Joi.valid('stale-if-slow', 'stale-while-revalidate', 'must-revalidate', 'no-cache');

const timezoneSchema = Joi.string().custom((value, helpers) => {
  const name = canonicalTimezone(value);
  if (!name) {
    return helpers.message({ custom: '{{#label}} must be a valid IANA time zone, got "{{#tz}}"' }, { tz: value });
  }

  return name;
}, 'timezone');

// It might be member name, td+granularity or member expression
const idOrMemberExpressionName = Joi.string().regex(/^[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+$|^[a-zA-Z0-9_]+$|^[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+$/);
const dimensionWithTime = Joi.string().regex(/^[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+(\.[a-zA-Z0-9_]+)?$/);
const parsedMemberExpression = Joi.object().keys({
  expression: Joi.alternatives(
    Joi.array().items(Joi.string()).min(1),
    parsedPatchMeasureExpression,
  ).required(),
  cubeName: Joi.string().required(),
  name: Joi.string().required(),
  expressionName: Joi.string(),
  definition: Joi.string(),
  groupingSet: Joi.object().keys({
    groupType: Joi.valid('Rollup', 'Cube').required(),
    id: Joi.number().required(),
    subId: Joi.number()
  })
});
const memberExpression = parsedMemberExpression.keys({
  expression: Joi.alternatives(
    Joi.func().required(),
    evaluatedPatchMeasureExpression,
  ).required(),
});

const inputSqlFunction = Joi.object().keys({
  cubeParams: Joi.array().items(Joi.string()).required(),
  sql: Joi.string().required(),
});

// This should be aligned with cubesql side
const inputMemberExpressionSqlFunction = inputSqlFunction.keys({
  type: Joi.valid('SqlFunction').required(),
});

// This should be aligned with cubesql side
const inputMemberExpressionPatchMeasure = Joi.object().keys({
  type: Joi.valid('PatchMeasure').required(),
  sourceMeasure: Joi.string().required(),
  replaceAggregationType: Joi.string().allow(null).required(),
  addFilters: Joi.array().items(inputSqlFunction).required(),
});

// This should be aligned with cubesql side
const inputMemberExpression = Joi.object().keys({
  cubeName: Joi.string().required(),
  alias: Joi.string().required(),
  expr: Joi.alternatives(
    inputMemberExpressionSqlFunction,
    inputMemberExpressionPatchMeasure,
  ),
  groupingSet: Joi.object().keys({
    groupType: Joi.valid('Rollup', 'Cube').required(),
    id: Joi.number().required(),
    subId: Joi.number().allow(null),
  }).allow(null)
});

const operators: string[] = [
  'equals',
  'notEquals',
  'contains',
  'notContains',
  'startsWith',
  'notStartsWith',
  'endsWith',
  'notEndsWith',
  'in',
  'notIn',
  'gt',
  'gte',
  'lt',
  'lte',
  'set',
  'notSet',
  'inDateRange',
  'notInDateRange',
  'onTheDate',
  'beforeDate',
  'beforeOrOnDate',
  'afterDate',
  'afterOrOnDate',
  'measureFilter',
];

const oneFilter = Joi.object().keys({
  dimension: id,
  member: id,
  operator: Joi.valid(...operators).required(),
  values: Joi.array().items(Joi.string().allow('', null), Joi.number(), Joi.boolean(), Joi.link('...'))
}).xor('dimension', 'member');

const oneCondition = Joi.object().keys({
  or: Joi.array().items(oneFilter, Joi.link('...').description('oneCondition schema')),
  and: Joi.array().items(oneFilter, Joi.link('...').description('oneCondition schema')),
}).xor('or', 'and');

const subqueryJoin = Joi.object().keys({
  sql: Joi.string(),
  // TODO This is _always_ a member expression, maybe pass as parsed, without intermediate string?
  // TODO there are three different types instead of alternatives for this actually
  on: Joi.alternatives(Joi.string(), memberExpression, parsedMemberExpression),
  joinType: Joi.string().valid('LEFT', 'INNER'),
  alias: Joi.string(),
});

const joinHint = Joi.array().items(Joi.string());

const querySchema = Joi.object().keys({
  // TODO add member expression alternatives only for SQL API queries?
  measures: Joi.array().items(Joi.alternatives(id, memberExpression, parsedMemberExpression)),
  dimensions: Joi.array().items(Joi.alternatives(dimensionWithTime, memberExpression, parsedMemberExpression)),
  filters: Joi.array().items(oneFilter, oneCondition),
  timeDimensions: Joi.array().items(Joi.object().keys({
    dimension: id.required(),
    granularity: Joi.string().max(128, 'utf8'), // Custom granularities may have arbitrary names
    dateRange: [
      Joi.array().items(Joi.string()).min(1).max(2),
      Joi.string()
    ],
    compareDateRange: Joi.array()
  }).oxor('dateRange', 'compareDateRange')),
  order: Joi.alternatives(
    Joi.object().pattern(idOrMemberExpressionName, Joi.valid('asc', 'desc')),
    Joi.array().items(Joi.array().min(2).ordered(idOrMemberExpressionName, Joi.valid('asc', 'desc')))
  ),
  segments: Joi.array().items(Joi.alternatives(id, memberExpression, parsedMemberExpression)),
  timezone: timezoneSchema,
  limit: Joi.number().integer().strict().min(0),
  offset: Joi.number().integer().strict().min(0),
  total: Joi.boolean(),
  cacheMode: cacheModeSchema,
  cache: cacheModeSchema,
  ungrouped: Joi.boolean(),
  responseFormat: Joi.valid('default', 'compact', 'columnar'),
  subqueryJoins: Joi.array().items(subqueryJoin),
  joinHints: Joi.array().items(joinHint),
  maskedMembers: Joi.array().items(Joi.object().keys({
    member: Joi.string().required(),
    filter: Joi.object(),
  })),
});

export const cubeSqlRequestSchema = Joi.object().keys({
  query: Joi.string().required(),
  timezone: timezoneSchema,
  cache: cacheModeSchema,
  throwContinueWait: Joi.boolean(),
});

// Joi accepts either `{ member: direction }` or `[[member, direction], ...]`
type QueryOrderInput = Record<string, string> | [string, string][];

const normalizeQueryOrder = (order: unknown): [string, string][] => {
  let result: [string, string][] = [];
  const normalizeOrderItem = (k: string, direction: string): [string, string] => ([k, direction]);
  if (order) {
    const input = order as QueryOrderInput;
    result = Array.isArray(input) ?
      input.map(([k, direction]) => normalizeOrderItem(k, direction)) :
      Object.keys(input).map(k => normalizeOrderItem(k, input[k]));
  }
  return result;
};

export const preAggsJobsRequestSchema = Joi.object({
  action: Joi.string().valid('post', 'get').required(),
  selector: Joi.when('action', {
    is: 'post',
    then: Joi.object({
      contexts: Joi.array().items(
        Joi.object({
          securityContext: Joi.required(),
        })
      ).min(1).required(),
      timezones: Joi.array().items(timezoneSchema).min(1).required(),
      dataSources: Joi.array().items(Joi.string()),
      cubes: Joi.array().items(Joi.string()),
      preAggregations: Joi.array().items(Joi.string()),
      dateRange: Joi.array().length(2).items(Joi.string()),
    }).optional(),
    otherwise: Joi.forbidden(),
  }),
  tokens: Joi.when('action', {
    is: 'get',
    then: Joi.array().items(Joi.string()).min(1).required(),
    otherwise: Joi.forbidden(),
  }),
  resType: Joi.when('action', {
    is: 'get',
    then: Joi.string().valid('object').optional(),
    otherwise: Joi.forbidden(),
  }),
});

const DateRegex = /^\d\d\d\d-\d\d-\d\d$/;

const DATE_RANGE_OPERATORS = ['inDateRange', 'notInDateRange'];
// Mirrors Tesseract's date_single.rs boundary semantics:
// Before → < start, AfterOrOn → >= start
const START_DATE_OPERATORS = ['beforeDate', 'afterOrOnDate'];
// BeforeOrOn → <= end, After → > end
const END_DATE_OPERATORS = ['beforeOrOnDate', 'afterDate'];

// Absolute values must pass through byte-exact: bare dates keep each planner's
// own day-boundary handling, timestamps keep their time component. Timestamps
// may carry a UTC designator or offset (e.g. from the SQL API push-down).
const AbsoluteDateTimeRegex = /^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2}(\.\d{1,6})?)?(Z|[+-]\d{2}(:?\d{2})?)?)?$/;

// Resolve a dateRange input — a relative string ("last 2 weeks"), a single
// absolute date, or a 2-element array — to a normalized [startISO, endISO]
// pair.
export function resolveDateRange(input: string | string[], timezone: string): string[];
export function resolveDateRange(input: unknown, timezone: string): unknown;
export function resolveDateRange(input: unknown, timezone: string): unknown {
  let dateRange: string[];
  if (typeof input === 'string') {
    dateRange = dateParser(input, timezone);
  } else if (Array.isArray(input)) {
    dateRange = input.length === 1 ? [input[0], input[0]] : input;
  } else {
    return input;
  }

  return dateRange.map(
    (d, i) => (
      i === 0 ?
        moment.utc(d).format(d.match(DateRegex) ? 'YYYY-MM-DDT00:00:00.000' : moment.HTML5_FMT.DATETIME_LOCAL_MS) :
        moment.utc(d).format(d.match(DateRegex) ? 'YYYY-MM-DDT23:59:59.999' : moment.HTML5_FMT.DATETIME_LOCAL_MS)
    )
  );
}

// Resolve relative date strings inside a filter leaf's `values` so that
// date-range filters can appear inside OR/AND groups (and at the top level)
// with the same relative-date support that `timeDimensions.dateRange` has.
// Reuses resolveDateRange so both paths produce identical output. Non-date
// operators and already-absolute values pass through unchanged.
export const normalizeDateFilterValues = <T extends { operator?: string, values?: unknown[] }>(filter: T, timezone: string): T => {
  if (!filter || !filter.operator || !Array.isArray(filter.values)) {
    return filter;
  }

  // Fail fast at the gateway if a range operator is given a multi-element
  // `values` array that contains a relative-date string. This shape falls
  // through the resolver (which only handles single-element values) and
  // would otherwise error deep in query execution with an opaque message. This
  // surfaces the failure at the API boundary instead.
  if (
    (DATE_RANGE_OPERATORS.includes(filter.operator) || filter.operator === 'onTheDate') &&
    filter.values.length > 1 &&
    filter.values.some((v) => typeof v === 'string' && !AbsoluteDateTimeRegex.test(v))
  ) {
    throw new UserError(
      `Relative-date strings are only supported when \`values\` has a single element for operator \`${filter.operator}\`. Pass an absolute two-element [start, end] pair, or a single relative string like ["last 2 weeks"]. Got: ${JSON.stringify(filter.values)}`
    );
  }

  if (filter.values.length !== 1) {
    return filter;
  }

  const value = filter.values[0];
  if (typeof value !== 'string') {
    return filter;
  }

  if (DATE_RANGE_OPERATORS.includes(filter.operator) || filter.operator === 'onTheDate') {
    // onTheDate resolves to a two-sided range: legacy onTheDateWhere reads
    // values[0]/values[1], and Tesseract maps onTheDate to InDateRange which
    // requires exactly 2 values.
    return { ...filter, values: resolveDateRange(value, timezone) };
  }

  if (AbsoluteDateTimeRegex.test(value)) {
    return filter;
  }

  if (START_DATE_OPERATORS.includes(filter.operator)) {
    const [start] = resolveDateRange(value, timezone);
    return { ...filter, values: [start] };
  }

  if (END_DATE_OPERATORS.includes(filter.operator)) {
    const [, end] = resolveDateRange(value, timezone);
    return { ...filter, values: [end] };
  }

  return filter;
};

type FilterInput = {
  or?: FilterInput[],
  and?: FilterInput[],
  dimension?: string,
  member?: string,
  operator?: string,
  values?: unknown[],
};

const normalizeQueryFilters = (filter: FilterInput[], timezone: string): FilterInput[] => (
  filter.map(f => {
    const res = { ...f };
    if (f.or) {
      res.or = normalizeQueryFilters(f.or, timezone);
      return res;
    }
    if (f.and) {
      res.and = normalizeQueryFilters(f.and, timezone);
      return res;
    }

    if (!f.operator) {
      throw new UserError(`Operator required for filter: ${JSON.stringify(f)}`);
    }

    if (operators.indexOf(f.operator) === -1) {
      throw new UserError(`Operator ${f.operator} not supported for filter: ${JSON.stringify(f)}`);
    }

    if ((!f.values || f.values.length === 0) && ['set', 'notSet', 'measureFilter'].indexOf(f.operator) === -1) {
      throw new UserError(`Values required for filter: ${JSON.stringify(f)}`);
    }

    if (f.values) {
      res.values = f.values.map(v => (v != null ? (v as { toString(): string }).toString() : v));
    }

    if (f.dimension) {
      res.member = f.dimension;
      delete res.dimension;
    }

    return normalizeDateFilterValues(res, timezone);
  })
);

/** @throws {UserError} */
function parseInputMemberExpression(expression: unknown): InputMemberExpression {
  const { error } = inputMemberExpression.validate(expression);
  if (error) {
    throw new UserError(`Invalid member expression format: ${error.message || error.toString()}`);
  }
  return expression as InputMemberExpression;
}

function normalizeQueryCacheMode(query: Query, cacheMode?: CacheMode): Query {
  if (cacheMode !== undefined) {
    query.cacheMode = cacheMode;
  } else if (!query.cache) {
    query.cacheMode = 'stale-if-slow';
  } else {
    query.cacheMode = query.cache;
  }

  query.cache = undefined;

  return query;
}

/** @throws {UserError} */
const normalizeQuery = (query: Query, persistent?: boolean, cacheMode?: CacheMode): NormalizedQuery => {
  query = normalizeQueryCacheMode(query, cacheMode);
  query.timezone = query.timezone || getEnv('defaultTimezone');
  const { error, value } = querySchema.validate(query);
  if (error) {
    throw new UserError(`Invalid query format: ${error.message || error.toString()}`);
  }

  const validQuery = query.measures?.length ||
    query.dimensions?.length ||
    query.timeDimensions?.filter(td => !!td.granularity).length;
  if (!validQuery) {
    throw new UserError(
      'Query should contain either measures, dimensions or timeDimensions with granularities in order to be valid'
    );
  }

  const regularToTimeDimension = (query.dimensions || []).filter((d): d is string => typeof d === 'string' && d.split('.').length === 3).map(d => ({
    dimension: d.split('.').slice(0, 2).join('.'),
    granularity: d.split('.')[2]
  }));
  const timezone = value.timezone || 'UTC';

  const def = getEnv('dbQueryDefaultLimit') <= getEnv('dbQueryLimit')
    ? getEnv('dbQueryDefaultLimit')
    : getEnv('dbQueryLimit');

  let newLimit: number | null | undefined;
  if (!persistent) {
    if (
      typeof query.limit === 'number' &&
      query.limit > getEnv('dbQueryLimit')
    ) {
      throw new Error('The query limit has been exceeded.');
    }
    newLimit = typeof query.limit === 'number'
      ? query.limit
      : def;
  } else {
    newLimit = query.limit;
  }

  return {
    ...query,
    // order stays in [member, direction] tuple form until remapToQueryAdapterFormat, despite NormalizedQuery['order']
    ...(query.order ? { order: normalizeQueryOrder(query.order) } : {}),
    limit: newLimit,
    timezone,
    // NormalizedQueryFilter does not model and/or groups
    filters: normalizeQueryFilters(query.filters || [], timezone) as NormalizedQueryFilter[],
    dimensions: (query.dimensions || []).filter(d => typeof d !== 'string' || d.split('.').length !== 3),
    // compareDateRange becomes an array of ranges here, which QueryTimeDimension does not model
    timeDimensions: (query.timeDimensions || []).map((td): any => {
      const compareDateRange = td.compareDateRange ? td.compareDateRange.map((currentDateRange) => (typeof currentDateRange === 'string' ? dateParser(currentDateRange, timezone) : currentDateRange)) : null;

      return {
        ...td,
        dateRange: resolveDateRange(td.dateRange, timezone),
        ...(compareDateRange ? { compareDateRange } : {})
      };
    }).concat(regularToTimeDimension)
  };
};

const remapQueryOrder = (order: unknown): { id: string, desc: boolean }[] => {
  let result: { id: string, desc: boolean }[] = [];
  const normalizeOrderItem = (k: string, direction: string) => ({
    id: k,
    desc: direction === 'desc'
  });
  if (order) {
    const input = order as QueryOrderInput;
    result = Array.isArray(input) ?
      input.map(([k, direction]) => normalizeOrderItem(k, direction)) :
      Object.keys(input).map(k => normalizeOrderItem(k, input[k]));
  }
  return result;
};

const remapToQueryAdapterFormat = (query: NormalizedQuery): NormalizedQuery => ({
  ...query,
  rowLimit: query.limit,
  ...(query.order ? { order: remapQueryOrder(query.order) } : {}),
});

type PreAggregationsQuery = {
  expand?: string[],
  metadata?: Record<string, unknown>,
  timezone?: string,
  timezones?: string[],
  preAggregations?: {
    id: string,
    cacheOnly?: boolean,
    metaOnly?: boolean,
    partitions?: string[],
    refreshRange?: [string, string],
  }[],
};

const queryPreAggregationsSchema = Joi.object<PreAggregationsQuery>().keys({
  expand: Joi.array().items(Joi.string()),
  metadata: Joi.object(),
  timezone: timezoneSchema,
  timezones: Joi.array().items(timezoneSchema),
  preAggregations: Joi.array().items(Joi.object().keys({
    id: Joi.string().required(),
    cacheOnly: Joi.boolean(),
    metaOnly: Joi.boolean(),
    partitions: Joi.array().items(Joi.string()),
    refreshRange: Joi.array().items(Joi.string()).length(2), // TODO: Deprecate after cloud changes
  }))
});

type PreAggregationPreviewQuery = {
  preAggregationId: string,
  timezone: string,
  versionEntry: {
    content_version?: string,
    last_updated_at?: number,
    naming_version?: number,
    structure_version?: string,
    table_name?: string,
    build_range_end?: string,
  },
};

const queryPreAggregationPreviewSchema = Joi.object<PreAggregationPreviewQuery>().keys({
  preAggregationId: Joi.string().required(),
  timezone: timezoneSchema.required(),
  versionEntry: Joi.object().required().keys({
    content_version: Joi.string(),
    last_updated_at: Joi.number(),
    naming_version: Joi.number(),
    structure_version: Joi.string(),
    table_name: Joi.string(),
    build_range_end: Joi.string(),
  })
});

type CancelPreAggregationsQuery = {
  dataSource?: string,
  queryKeys?: string[],
};

const queryCancelPreAggregationPreviewSchema = Joi.object<CancelPreAggregationsQuery>().keys({
  dataSource: Joi.string(),
  queryKeys: Joi.array().items(Joi.string())
});

/** @throws {UserError} */
function validateQuery<T>(schema: Joi.ObjectSchema<T>, query: unknown): T {
  const { error, value } = schema.validate(query);
  if (error) {
    throw new UserError(`Invalid query format: ${error.message || error.toString()}`);
  }

  return value;
}

const normalizeQueryPreAggregations = (query: unknown, defaultValues?: { timezones?: string[] }) => {
  const { metadata, timezone, timezones, preAggregations, expand } = validateQuery(queryPreAggregationsSchema, query);

  return {
    metadata,
    timezones: timezones || (timezone && [timezone]) || defaultValues?.timezones || ['UTC'],
    preAggregations,
    expand
  };
};

const normalizeQueryPreAggregationPreview = (query: unknown): PreAggregationPreviewQuery => validateQuery(queryPreAggregationPreviewSchema, query);

const normalizeQueryCancelPreAggregations = (query: unknown): CancelPreAggregationsQuery => validateQuery(queryCancelPreAggregationPreviewSchema, query);

export {
  getQueryGranularity,
  getPivotQuery,
  normalizeQuery,
  normalizeQueryPreAggregations,
  normalizeQueryPreAggregationPreview,
  normalizeQueryCancelPreAggregations,
  parseInputMemberExpression,
  remapToQueryAdapterFormat,
};
