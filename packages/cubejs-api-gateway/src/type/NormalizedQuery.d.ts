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
import Query from './Query';

/**
 * Network query data type.
 */
type NormalizedQuery = Query & {
  rowLimit?: number,
  order?: { id: Id, desc: boolean },
  filters?: Filter[],
  timeDimensions?: TimeDimension[],
};

export default NormalizedQuery;
