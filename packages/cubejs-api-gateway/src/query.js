import R from 'ramda';
import moment from 'moment';
import Joi from '@hapi/joi';

import { UserError } from './UserError';
import { dateParser } from './dateParser';

export const QUERY_TYPE = {
  REGULAR_QUERY: 'regularQuery',
  COMPARE_DATE_RANGE_QUERY: 'compareDateRangeQuery',
  BLENDING_QUERY: 'blendingQuery',
};

export const getQueryGranularity = (queries) => R.pipe(
  R.map(({ timeDimensions }) => timeDimensions[0] && timeDimensions[0].granularity || null),
  R.filter(Boolean),
  R.uniq
)(queries);

export const getPivotQuery = (queryType, queries) => {
  let [pivotQuery] = queries;

  if (queryType === QUERY_TYPE.BLENDING_QUERY) {
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
  } else if (queryType === QUERY_TYPE.COMPARE_DATE_RANGE_QUERY) {
    pivotQuery.dimensions = ['compareDateRange'].concat(pivotQuery.dimensions || []);
  }

  pivotQuery.queryType = queryType;

  return pivotQuery;
};

const id = Joi.string().regex(/^[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+$/);
const dimensionWithTime = Joi.string().regex(/^[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+(\.(second|minute|hour|day|week|month|year))?$/);

const operators = [
  'equals',
  'notEquals',
  'contains',
  'notContains',
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
  'afterDate',
  'measureFilter',
];

const oneFilter = Joi.object().keys({
  dimension: id,
  member: id,
  operator: Joi.valid(operators).required(),
  values: Joi.array().items(Joi.string().allow('', null), Joi.lazy(() => oneFilter))
}).xor('dimension', 'member');

const oneCondition = Joi.object().keys({
  or: Joi.array().items(oneFilter, Joi.lazy(() => oneCondition).description('oneCondition schema')),
  and: Joi.array().items(oneFilter, Joi.lazy(() => oneCondition).description('oneCondition schema')),
}).xor('or', 'and');

const querySchema = Joi.object().keys({
  measures: Joi.array().items(id),
  dimensions: Joi.array().items(dimensionWithTime),
  filters: Joi.array().items(oneFilter, oneCondition),
  timeDimensions: Joi.array().items(Joi.object().keys({
    dimension: id.required(),
    granularity: Joi.valid('day', 'month', 'year', 'week', 'hour', 'minute', 'second', null),
    dateRange: [
      Joi.array().items(Joi.string()).min(1).max(2),
      Joi.string()
    ],
    compareDateRange: Joi.array()
  }).oxor('dateRange', 'compareDateRange')),
  order: Joi.alternatives(
    Joi.object().pattern(id, Joi.valid('asc', 'desc')),
    Joi.array().items(Joi.array().min(2).ordered(id, Joi.valid('asc', 'desc')))
  ),
  segments: Joi.array().items(id),
  timezone: Joi.string(),
  limit: Joi.number().integer().min(1).max(50000),
  offset: Joi.number().integer().min(0),
  renewQuery: Joi.boolean(),
  ungrouped: Joi.boolean()
});

const normalizeQueryOrder = order => {
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

const DateRegex = /^\d\d\d\d-\d\d-\d\d$/;

const checkQueryFilters = (filter) => {
  filter.find(f => {
    if (f.or) {
      checkQueryFilters(f.or);
      return false;
    }
    if (f.and) {
      checkQueryFilters(f.and);
      return false;
    }

    if (!f.operator) {
      throw new UserError(`Operator required for filter: ${JSON.stringify(f)}`);
    }

    if (operators.indexOf(f.operator) === -1) {
      throw new UserError(`Operator ${f.operator} not supported for filter: ${JSON.stringify(f)}`);
    }

    if (!f.values && ['set', 'notSet', 'measureFilter'].indexOf(f.operator) === -1) {
      throw new UserError(`Values required for filter: ${JSON.stringify(f)}`);
    }
    return false;
  });

  return true;
};

export const normalizeQuery = (query) => {
  const { error } = Joi.validate(query, querySchema);
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

  checkQueryFilters(query.filters || []);

  const regularToTimeDimension = (query.dimensions || []).filter(d => d.split('.').length === 3).map(d => ({
    dimension: d.split('.').slice(0, 2).join('.'),
    granularity: d.split('.')[2]
  }));
  const timezone = query.timezone || 'UTC';
  return {
    ...query,
    rowLimit: query.rowLimit || query.limit,
    timezone,
    order: normalizeQueryOrder(query.order),
    filters: (query.filters || []).map(f => {
      const { dimension, member, ...filter } = f;
      const normalizedFlter = {
        ...filter,
        member: member || dimension
      };

      Object.defineProperty(normalizedFlter, 'dimension', {
        get() {
          console.warn('Warning: Attribute `filter.dimension` is deprecated. Please use \'member\' instead of \'dimension\'.');
          return this.member;
        }
      });
      return normalizedFlter;
    }),
    dimensions: (query.dimensions || []).filter(d => d.split('.').length !== 3),
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

const queryPreAggregationsSchema = Joi.object().keys({
  timezone: Joi.string(),
  timezones: Joi.array().items(Joi.string()),
  preAggregations: Joi.array().items(Joi.object().keys({
    id: Joi.string().required(),
    refreshRange: Joi.array().items(Joi.string()).length(2)
  }))
});

export const normalizeQueryPreAggregations = (query, defaultValues) => {
  const { error } = Joi.validate(query, queryPreAggregationsSchema);
  if (error) {
    throw new UserError(`Invalid query format: ${error.message || error.toString()}`);
  }

  return {
    timezones: query.timezones || (query.timezone && [query.timezone]) || defaultValues.timezones,
    preAggregations: query.preAggregations
  };
};
