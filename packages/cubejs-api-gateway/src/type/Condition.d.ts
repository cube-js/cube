/**
 * @fileoverview Network query condition data type definition.
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 */

import Filter from './Filter';

/**
 * Network query base condition data type.
 */
type BaseCondition = {
  or?: Filter[] | BaseCondition[],
  and?: Filter[] | BaseCondition[],
};

/**
 * Network query or-condition data type.
 */
type OrCondition = BaseCondition & {
  or: Filter[] | BaseCondition[],
  and: undefined,
};

/**
 * Network query and-condition data type.
 */
type AndCondition = BaseCondition & {
  or: undefined,
  and: Filter[] | BaseCondition[],
};

/**
 * Network query condition data type.
 */
type Condition = OrCondition | AndCondition;

export default Condition;
export {
  BaseCondition,
  OrCondition,
  AndCondition,
  Condition,
};
