/**
 * @license Apache-2.0
 * @copyright Cube Dev, Inc.
 * @fileoverview
 * API Gateway enums definition.
 */

/**
 * Query type enum.
 */
enum QueryType {
  REGULAR_QUERY = 'regularQuery',
  COMPARE_DATE_RANGE_QUERY = 'compareDateRangeQuery',
  BLENDING_QUERY = 'blendingQuery',
}

/**
 * Query result dataset formats enum.
 */
enum ResultType {
  DEFAULT = 'default',
  COMPACT = 'compact'
}

/**
 * Network query order types enum.
 */
enum OrderType {
  ASC = 'asc',
  DESC = 'desc',
}

/**
 * String that represent query member type.
 */
enum MemberType {
  MEASURES = 'measures',
  DIMENSIONS = 'dimensions',
  SEGMENTS = 'segments',
}

export {
  MemberType,
  OrderType,
  QueryType,
  ResultType,
};
