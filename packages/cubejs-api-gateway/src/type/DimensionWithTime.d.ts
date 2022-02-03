/**
 * @fileoverview Network query dimension data type definition.
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 */

/**
 * Datetime dimensions name string. Should satisfy to the following
 * regexp:
 * {@code /^[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+(\.(second|minute|hour|day|week|month|year))?$/}.
 */
type DimensionWithTime = string;

export default DimensionWithTime;
