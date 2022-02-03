/**
 * @fileoverview Network query filters data type definition.
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 */

import Id from './Id';
import Operator from './Operator';

/**
 * Base filter structure.
 */
type BaseFilter = {
  operator: Operator,
  values: (null | string | BaseFilter)[],
};

/**
 * Dimension filter structure.
 */
type DimensionFilter = BaseFilter & {
  dimension: Id,
};

/**
 * Member filter struncture.
 */
type MemberFilter = BaseFilter & {
  member: Id,
};

/**
 * Filter structure.
 */
type Filter = DimensionFilter | MemberFilter;

export default Filter;
export {
  BaseFilter,
  DimensionFilter,
  MemberFilter,
  Filter,
};
