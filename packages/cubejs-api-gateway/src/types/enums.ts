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

export {
  QueryType,
};
