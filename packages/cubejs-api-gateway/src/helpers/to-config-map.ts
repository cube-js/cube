/**
 * @license Apache-2.0
 * @copyright Cube Dev, Inc.
 * @fileoverview
 * toConfigMap function and related types definition.
 */

import R from 'ramda';

/**
 * MetaConfig type.
 */
type MetaConfig = {
  config: {
    name: string,
    title: string,
  }
};

/**
 * MetaConfig Map type.
 */
type MetaConfigMap = {
  [name: string]: {
    name: string,
    title: string,
  },
};

/**
 * Convert specified array of MetaConfig objects to the
 * MetaConfigMap.
 */
function toConfigMap(metaConfig: MetaConfig[]): MetaConfigMap {
  return R.fromPairs(
    R.map(
      (c) => [c.config.name, c.config],
      metaConfig
    )
  );
}

export default toConfigMap;
export {
  MetaConfig,
  MetaConfigMap,
  toConfigMap,
};
