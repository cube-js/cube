/**
 * @fileoverview Network query order data types definition.
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 */

import OrderType from '../enum/OrderType';

/**
 * Network query order data type.
 */
type Order = {
  [member: string]: OrderType
};

export default Order;
