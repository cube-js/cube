/**
 * @fileoverview Network query filter operator data type definition.
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 */

/**
 * String that represent filter operator.
 */
type Operator =
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
  'afterDate' |
  'measureFilter';

export default Operator;
