import crypto from 'crypto';

import type { GlobalGranularitiesConfig } from './GlobalGranularitiesConfig';

// Emission-order-sensitive canonical form: `enabledBuiltIns` order and custom-granularity
// insertion order both affect the resolved set that clients receive, so neither is sorted.
// Only the known serializable fields participate; anything else a config spreads onto a
// definition (including functions) is ignored so the hash stays deterministic.
const asHashableString = (value: unknown): string | undefined => (
  typeof value === 'string' ? value : undefined
);

// Canonical sha256 of a resolved global granularities config. Two configs share a hash iff
// they produce identical effective granularity sets, so the hash is safe to use as a cache
// key for enriched meta variants and as a compilerId discriminator.
export function granularityConfigHash(config: GlobalGranularitiesConfig): string {
  const canonical = {
    builtIns: [...config.enabledBuiltIns],
    custom: Object.entries(config.customGranularities).map(([name, def]) => ({
      name,
      title: asHashableString(def.title),
      format: asHashableString(def.format),
      interval: asHashableString(def.interval),
      offset: asHashableString(def.offset),
      origin: asHashableString(def.origin),
    })),
  };
  return crypto.createHash('sha256').update(JSON.stringify(canonical)).digest('hex');
}
