import R from 'ramda';
import moment from 'moment';
import Joi from 'joi';
import { getEnv } from '@cubejs-backend/shared';

import { UserError } from './UserError';
import { dateParser } from './dateParser';
import { QueryType } from './types/enums';

const getQueryGranularity = (queries) => R.pipe(
  R.map(({ timeDimensions }) => timeDimensions[0] && timeDimensions[0].granularity || null),
  R.filter(Boolean),
  R.uniq
)(queries);

const getPivotQuery = (queryType, queries) => {
  let [pivotQuery] = queries;

  if (queryType === QueryType.BLENDING_QUERY) {
    pivotQuery = R.fromPairs(
      ['measures', 'dimensions'].map(
        (key) => [key, R.uniq(queries.reduce((memo, q) => memo.concat(q[key]), []))]
      )
    );

    const [granularity] = getQueryGranularity(queries);

    pivotQuery.timeDimensions = [{
      dimension: 'time',
      granularity
    }];
  } else if (queryType === QueryType.COMPARE_DATE_RANGE_QUERY) {
    pivotQuery.dimensions = ['compareDateRange'].concat(pivotQuery.dimensions || []);
  }

  pivotQuery.queryType = queryType;

  return pivotQuery;
};

const id = Joi.string().regex(/^[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+$/);
const idOrMemberExpressionName = Joi.string().regex(/^[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+$|^[a-zA-Z0-9_]+$/);
const dimensionWithTime = Joi.string().regex(/^[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+(\.[a-zA-Z0-9_]+)?$/);
const parsedMemberExpression = Joi.object().keys({
  expression: Joi.array().items(Joi.string()).min(1).required(),
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
  expression: Joi.func().required(),
});

const operators = [
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
  timezone: Joi.string(),
  limit: Joi.number().integer().min(0),
  offset: Joi.number().integer().min(0),
  total: Joi.boolean(),
  renewQuery: Joi.boolean(),
  ungrouped: Joi.boolean(),
  responseFormat: Joi.valid('default', 'compact'),
  subqueryJoins: Joi.array().items(subqueryJoin),
});

const normalizeQueryOrder = order => {
  let result = [];
  const normalizeOrderItem = (k, direction) => ([k, direction]);
  if (order) {
    result = Array.isArray(order) ?
      order.map(([k, direction]) => normalizeOrderItem(k, direction)) :
      Object.keys(order).map(k => normalizeOrderItem(k, order[k]));
  }
  return result;
};

const DateRegex = /^\d\d\d\d-\d\d-\d\d$/;

const normalizeQueryFilters = (filter) => (
  filter.map(f => {
    const res = { ...f };
    if (f.or) {
      res.or = normalizeQueryFilters(f.or);
      return res;
    }
    if (f.and) {
      res.and = normalizeQueryFilters(f.and);
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
      res.values = f.values.map(v => (v != null ? v.toString() : v));
    }

    if (f.dimension) {
      res.member = f.dimension;
      delete res.dimension;
    }

    return res;
  })
);

/**
 * Normalize incoming network query.
 * @param {Query} query
 * @param {boolean} persistent
 * @throws {UserError}
 * @returns {NormalizedQuery}
 */
const normalizeQuery = (query, persistent) => {
  const { error } = querySchema.validate(query);
  if (error) {
    throw new UserError(`Invalid query format: ${error.message || error.toString()}`);
  }
  const validQuery = query.measures && query.measures.length ||
    query.dimensions && query.dimensions.length ||
    query.timeDimensions && query.timeDimensions.filter(td => !!td.granularity).length;
  if (!validQuery) {
    throw new UserError(
      'Query should contain either measures, dimensions or timeDimensions with granularities in order to be valid'
    );
  }

  const regularToTimeDimension = (query.dimensions || []).filter(d => typeof d === 'string' && d.split('.').length === 3).map(d => ({
    dimension: d.split('.').slice(0, 2).join('.'),
    granularity: d.split('.')[2]
  }));
  const timezone = query.timezone || 'UTC';

  const def = getEnv('dbQueryDefaultLimit') <= getEnv('dbQueryLimit')
    ? getEnv('dbQueryDefaultLimit')
    : getEnv('dbQueryLimit');

  let newLimit;
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
    ...(query.order ? { order: normalizeQueryOrder(query.order) } : {}),
    limit: newLimit,
    timezone,
    filters: normalizeQueryFilters(query.filters || []),
    dimensions: (query.dimensions || []).filter(d => typeof d !== 'string' || d.split('.').length !== 3),
    timeDimensions: (query.timeDimensions || []).map(td => {
      let dateRange;

      const compareDateRange = td.compareDateRange ? td.compareDateRange.map((currentDateRange) => (typeof currentDateRange === 'string' ? dateParser(currentDateRange, timezone) : currentDateRange)) : null;

      if (typeof td.dateRange === 'string') {
        dateRange = dateParser(td.dateRange, timezone);
      } else {
        dateRange = td.dateRange && td.dateRange.length === 1 ? [td.dateRange[0], td.dateRange[0]] : td.dateRange;
      }
      return {
        ...td,
        dateRange: dateRange && dateRange.map(
          (d, i) => (
            i === 0 ?
              moment.utc(d).format(d.match(DateRegex) ? 'YYYY-MM-DDT00:00:00.000' : moment.HTML5_FMT.DATETIME_LOCAL_MS) :
              moment.utc(d).format(d.match(DateRegex) ? 'YYYY-MM-DDT23:59:59.999' : moment.HTML5_FMT.DATETIME_LOCAL_MS)
          )
        ),
        ...(compareDateRange ? { compareDateRange } : {})
      };
    }).concat(regularToTimeDimension)
  };
};

const remapQueryOrder = order => {
  let result = [];
  const normalizeOrderItem = (k, direction) => ({
    id: k,
    desc: direction === 'desc'
  });
  if (order) {
    result = Array.isArray(order) ?
      order.map(([k, direction]) => normalizeOrderItem(k, direction)) :
      Object.keys(order).map(k => normalizeOrderItem(k, order[k]));
  }
  return result;
};

const remapToQueryAdapterFormat = (query) => (query ? {
  ...query,
  rowLimit: query.limit,
  ...(query.order ? { order: remapQueryOrder(query.order) } : {}),
} : query);

const queryPreAggregationsSchema = Joi.object().keys({
  expand: Joi.array().items(Joi.string()),
  metadata: Joi.object(),
  timezone: Joi.string(),
  timezones: Joi.array().items(Joi.string()),
  preAggregations: Joi.array().items(Joi.object().keys({
    id: Joi.string().required(),
    cacheOnly: Joi.boolean(),
    partitions: Joi.array().items(Joi.string()),
    refreshRange: Joi.array().items(Joi.string()).length(2), // TODO: Deprecate after cloud changes
  }))
});

const normalizeQueryPreAggregations = (query, defaultValues) => {
  const { error } = queryPreAggregationsSchema.validate(query);
  if (error) {
    throw new UserError(`Invalid query format: ${error.message || error.toString()}`);
  }

  return {
    metadata: query.metadata,
    timezones: query.timezones || (query.timezone && [query.timezone]) || defaultValues?.timezones || ['UTC'],
    preAggregations: query.preAggregations,
    expand: query.expand
  };
};

const queryPreAggregationPreviewSchema = Joi.object().keys({
  preAggregationId: Joi.string().required(),
  timezone: Joi.string().required(),
  versionEntry: Joi.object().required().keys({
    content_version: Joi.string(),
    last_updated_at: Joi.number(),
    naming_version: Joi.number(),
    structure_version: Joi.string(),
    table_name: Joi.string(),
    build_range_end: Joi.string(),
  })
});

const normalizeQueryPreAggregationPreview = (query) => {
  const { error } = queryPreAggregationPreviewSchema.validate(query);
  if (error) {
    throw new UserError(`Invalid query format: ${error.message || error.toString()}`);
  }

  return query;
};

const queryCancelPreAggregationPreviewSchema = Joi.object().keys({
  dataSource: Joi.string(),
  queryKeys: Joi.array().items(Joi.string())
});

const normalizeQueryCancelPreAggregations = query => {
  const { error } = queryCancelPreAggregationPreviewSchema.validate(query);
  if (error) {
    throw new UserError(`Invalid query format: ${error.message || error.toString()}`);
  }

  return query;
};

export {
  getQueryGranularity,
  getPivotQuery,
  normalizeQuery,
  normalizeQueryPreAggregations,
  normalizeQueryPreAggregationPreview,
  normalizeQueryCancelPreAggregations,
  remapToQueryAdapterFormat,
};
