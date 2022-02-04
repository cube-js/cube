/**
 * @license Apache-2.0
 * @copyright Cube Dev, Inc.
 * @fileoverview
 * transformValue function and related types definition.
 */

import moment from 'moment';

/**
 * Transform cpecified `value` with specified `type` to the network
 * protocol type.
 */
function transformValue(value: any, type: string) {
  // TODO: support for max time
  if (value && (type === 'time' || value instanceof Date)) {
    return (
      value instanceof Date
        ? moment(value)
        : moment.utc(value)
    ).format(moment.HTML5_FMT.DATETIME_LOCAL_MS);
  }
  // TODO: move to sql adapter
  return value && value.value ? value.value : value;
}

export default transformValue;
export {
  transformValue,
};
