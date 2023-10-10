/**
 * @license Apache-2.0
 * @copyright Cube Dev, Inc.
 * @fileoverview
 * Network request predefined strings data types definition.
 */

/**
 * Request type data type.
 */
type RequestType =
  'multi';

/**
 * Result type data type.
 */
type ResultType =
  'default' |
  'compact';

/**
 * API type data type.
 */
type ApiType =
  'sql' |
  'graphql' |
  'rest' |
  'ws' |
  'stream';

/**
 * Parsed query type data type.
 */
type QueryType =
  'regularQuery' |
  'compareDateRangeQuery' |
  'blendingQuery';

/**
 * String that represent query member type.
 */
type MemberType =
  'measures' |
  'dimensions' |
  'segments';

/**
 * Member identifier. Should satisfy to the following regexp: /^[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+$/
 */
type Member = string;

/**
 * Datetime member identifier. Should satisfy to the following
 * regexp: /^[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+(\.(second|minute|hour|day|week|month|year))?$/
 */
type TimeMember = string;

/**
 * Filter operator string.
 */
type FilterOperator =
 'equals' |
 'notEquals' |
 'contains' |
 'notContains' |
 'in' |
 'notIn' |
 'gt' |
 'gte' |
 'lt' |
 'lte' |
 'set' |
 'notSet' |
 'inDateRange' |
 'notInDateRange' |
 'onTheDate' |
 'beforeDate' |
 'beforeOrOnDate' |
 'afterDate' |
 'afterOrOnDate' |
 'measureFilter';

/**
 * Time dimension granularity data type.
 */
type QueryTimeDimensionGranularity =
 'quarter' |
 'day' |
 'month' |
 'year' |
 'week' |
 'hour' |
 'minute' |
 'second';

/**
 * Query order data type.
 */
type QueryOrderType =
  'asc' |
  'desc';

/**
 * ApiScopes data type.
 */
type ApiScopes =
  'graphql' |
  'meta' |
  'data' |
  'jobs';

export {
  RequestType,
  ResultType,
  ApiType,
  QueryType,
  MemberType,
  Member,
  TimeMember,
  FilterOperator,
  QueryTimeDimensionGranularity,
  QueryOrderType,
  ApiScopes,
};
