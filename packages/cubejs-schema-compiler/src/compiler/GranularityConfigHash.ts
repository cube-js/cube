import crypto from 'crypto';

import type { GlobalGranularitiesConfig } from './GlobalGranularitiesConfig';

// Only known serializable fields participate, so the hash stays deterministic even when a
// config spreads extra props (e.g. functions) onto a definition.
const asHashableString = (value: unknown): string | undefined => (
  typeof value === 'string' ? value : undefined
);

// Canonical sha256 of a resolved global granularities config: equal hash iff identical effective
// sets. Order-sensitive on purpose — built-in and custom order affect the emitted meta.
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
