/**
 * @fileoverview Network query data type definition.
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 */

import Id from './Id';
import DimensionWithTime from './DimensionWithTime';
import Filter from './Filter';
import Condition from './Condition';
import TimeDimension from './TimeDimension';
import Order from './Order';

/**
 * Network query data type.
 */
type Query = {
  measures?: Id[],
  dimensions?: DimensionWithTime[],
  filters?: Filter[] | Condition[],
  timeDimensions?: TimeDimension[],
  order?: Order | Order[],
  segments?: Id[],
  timezone?: string,
  limit?: number,
  offset?: number,
  renewQuery?: boolean,
  ungrouped?: boolean,
};

export default Query;
