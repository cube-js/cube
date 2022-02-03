/**
 * @fileoverview Network query identifier data type definition.
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 */

/**
 * Unique identifier. Should satisfy to the following regexp:
 * {@code /^[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+$/}.
 */
type Id = string;

export default Id;
