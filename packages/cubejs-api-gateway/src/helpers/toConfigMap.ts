import R from 'ramda';

export

/**
 * MetaConfig type.
 */
type MetaConfig = {
  config: {
    name: string,
    title: string,
  }
};

export

/**
 * MetaConfig Map type.
 */
type MetaConfigMap = {
  [name: string]: {
    name: string,
    title: string,
  },
};

export

/**
 * Convert cpecified array of MetaConfig objects to the
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
