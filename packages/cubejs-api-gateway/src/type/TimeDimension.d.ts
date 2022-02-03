/**
 * @fileoverview Network query time dimension data types definition.
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 */

import Id from './Id';

/**
 * Time dimension granularity data type.
 */
type Granularity =
  'quarter' |
  'day' |
  'month' |
  'year' |
  'week' |
  'hour' |
  'minute' |
  'second';

/**
 * Time dimension date range data type.
 */
type DateRange = [
  [string, string | undefined],
  string,
];

/**
 * Base time dimension data type.
 */
type BaseTimeDim = {
  dimension: Id,
  granularity?: null | Granularity,
};

/**
 * Date range dimension data type.
 */
type DateRangeDim = BaseTimeDim & {
  dateRange: DateRange,
};

/**
 * Date range dimension data type.
 */
type CompareDateRangeDim = BaseTimeDim & {
  compareDateRange: [],
};

/**
 * Network query time dimension data type.
 */
type TimeDimension = BaseTimeDim | DateRangeDim | CompareDateRangeDim;

export default TimeDimension;
export {
  BaseTimeDim,
  DateRangeDim,
  CompareDateRangeDim,
  TimeDimension,
};
