/**
 * @license Apache-2.0
 * @copyright Cube Dev, Inc.
 * @fileoverview
 * transformValue function and related types definition.
 */

import moment, { MomentInput } from 'moment';

/**
 * Query 'or'-filters type definition.
 */
type DBResponsePrimitive =
  null |
  boolean |
  number |
  string;

type DBResponseValue =
  Date |
  DBResponsePrimitive |
  { value: DBResponsePrimitive };

/**
 * Transform cpecified `value` with specified `type` to the network
 * protocol type.
 */
function transformValue(
  value: DBResponseValue,
  type: string
): DBResponsePrimitive {
  // TODO: support for max time
  if (value && (type === 'time' || value instanceof Date)) {
    return (
      value instanceof Date
        ? moment(value)
        : moment.utc(value as MomentInput)
    ).format(moment.HTML5_FMT.DATETIME_LOCAL_MS);
  }
  // TODO: move to sql adapter
  return value && (value as { value: DBResponsePrimitive }).value
    ? (value as { value: DBResponsePrimitive }).value
    : value as DBResponsePrimitive;
}

export default transformValue;
export {
  DBResponsePrimitive,
  DBResponseValue,
  transformValue,
};
