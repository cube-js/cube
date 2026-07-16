import crypto from 'crypto';
import { getEnv } from '@cubejs-backend/shared';

import type { GranularityDefinition } from './CubeSymbols';

// Default `title` and `format` for each built-in granularity. Overridable via `config.granularities`
// (file) or `CUBEJS_GRANULARITIES_<NAME>_TITLE` (env, title only). Format syntax: d3-time-format.

export type BuiltInGranularityDefinition = {
  title: string;
  format: string;
};

export const BUILT_IN_GRANULARITIES: Readonly<Record<string, BuiltInGranularityDefinition>> = Object.freeze({
  year: { title: 'Year', format: '%Y' },
  quarter: { title: 'Quarter', format: 'Q%q %Y' },
  month: { title: 'Month', format: '%b %Y' },
  week: { title: 'Week', format: '%b %-d, %Y' },
  day: { title: 'Day', format: '%Y-%m-%d' },
  hour: { title: 'Hour', format: '%Y-%m-%d %H:00' },
  minute: { title: 'Minute', format: '%Y-%m-%d %H:%M' },
  second: { title: 'Second', format: '%Y-%m-%d %H:%M:%S' },
});

export const BUILT_IN_GRANULARITY_NAMES = Object.freeze(Object.keys(BUILT_IN_GRANULARITIES));

export function isBuiltInGranularity(name: string): boolean {
  // hasOwnProperty, not `in`: `'__proto__' in {}` / `'constructor' in {}` are true via the
  // prototype chain, which would misclassify those names as built-in granularities.
  return Object.prototype.hasOwnProperty.call(BUILT_IN_GRANULARITIES, name);
}

// Item shape accepted in `config.granularities`: a built-in name, or a custom granularity object.
export type GranularityListItem = string | (GranularityDefinition & { name: string });
export type GranularityList = GranularityListItem[];

export type GlobalGranularitiesConfig = {
  enabledBuiltIns: ReadonlyArray<string>;
  customGranularities: Readonly<Record<string, GranularityDefinition>>;
};

const DEFAULT_CONFIG: GlobalGranularitiesConfig = Object.freeze({
  enabledBuiltIns: BUILT_IN_GRANULARITY_NAMES,
  customGranularities: Object.freeze({}),
});

// Read `CUBEJS_GRANULARITIES_<NAME>_{INTERVAL,TITLE,OFFSET,ORIGIN}` for a custom granularity name.
// Only consulted for names sourced from `CUBEJS_GRANULARITIES`, not for `config.granularities` entries.
function applyEnvOverrides(name: string, base?: Partial<GranularityDefinition>): GranularityDefinition {
  // getEnv types `opts` as a Parameters<> tuple but forwards it positionally; cast to bypass that.
  const opts = { name } as any;
  const interval = getEnv('granularityCustomInterval', opts) ?? base?.interval;
  const title = getEnv('granularityCustomTitle', opts) ?? base?.title;
  const offset = getEnv('granularityCustomOffset', opts) ?? base?.offset;
  const origin = getEnv('granularityCustomOrigin', opts) ?? base?.origin;

  const out: GranularityDefinition = {};
  if (interval !== undefined) out.interval = interval;
  if (title !== undefined) out.title = title;
  if (offset !== undefined) out.offset = offset;
  if (origin !== undefined) out.origin = origin;
  return out;
}

function resolveFromEnv(): GlobalGranularitiesConfig {
  const list = getEnv('granularities');
  if (!list || list.length === 0) {
    return DEFAULT_CONFIG;
  }

  const enabledBuiltIns: string[] = [];
  const customGranularities: Record<string, GranularityDefinition> = {};
  for (const name of list) {
    const trimmed = name.trim();
    if (trimmed) {
      if (isBuiltInGranularity(trimmed)) {
        enabledBuiltIns.push(trimmed);
        // A built-in may still carry per-name env overrides (title/format via _TITLE etc.); fold
        // them into customGranularities so buildBuiltInsCatalog picks them up (built-in stays built-in).
        const override = applyEnvOverrides(trimmed);
        if (Object.keys(override).length > 0) {
          customGranularities[trimmed] = override;
        }
      } else {
        // Non-built-in name: pull the definition from `CUBEJS_GRANULARITIES_<NAME>_*` env vars.
        // Skip if no interval was provided — a custom granularity without an interval is unusable.
        const def = applyEnvOverrides(trimmed);
        if (def.interval !== undefined) {
          customGranularities[trimmed] = def;
        }
      }
    }
  }
  return { enabledBuiltIns, customGranularities };
}

// Project a definition onto exactly the known string fields, dropping anything else (functions,
// nested objects, numbers). Guarantees every value the serializer emits and the hash reads is a
// plain string, so the two can never diverge (no cross-tenant cache-key collision), and stray
// props a config spreads in can't leak onto the wire.
function sanitizeDefinition(def: Partial<GranularityDefinition>): GranularityDefinition {
  const out: GranularityDefinition = {};
  for (const key of ['title', 'format', 'interval', 'offset', 'origin'] as const) {
    if (typeof def[key] === 'string') {
      out[key] = def[key];
    }
  }
  return out;
}

function resolveFromList(list: GranularityList): GlobalGranularitiesConfig {
  const enabledBuiltIns: string[] = [];
  const customGranularities: Record<string, GranularityDefinition> = {};

  for (const item of list) {
    if (typeof item === 'string') {
      if (isBuiltInGranularity(item)) {
        enabledBuiltIns.push(item);
      }
      // A bare non-built-in string has no definition attached and is silently dropped:
      // custom granularities in `config.granularities` must be objects.
    } else if (item && typeof item === 'object' && item.name) {
      const { name, ...def } = item;
      const clean = sanitizeDefinition(def);
      if (isBuiltInGranularity(name)) {
        // `{ name: 'year', title: 'Anno' }` both enables 'year' and overrides its title/format.
        enabledBuiltIns.push(name);
        customGranularities[name] = clean;
      } else if (clean.interval !== undefined) {
        // A custom granularity without an interval is unusable (SQL can't bucket it) and must not
        // be advertised — drop it rather than exposing a granularity that fails at query time.
        customGranularities[name] = clean;
      }
    }
  }
  return { enabledBuiltIns, customGranularities };
}

// Config forms accepted for `granularities` in cube.js / cube.py.
export type GranularitiesOption =
  GranularityList | ((ctx: any) => GranularityList | Promise<GranularityList>) | undefined;

// Sync resolution for the context-independent forms (undefined -> env, static list).
// The function form is per-context and must go through `resolveGlobalGranularities`.
export function resolveGlobalGranularitiesSync(
  userValue: Exclude<GranularitiesOption, Function>,
): GlobalGranularitiesConfig {
  if (!Array.isArray(userValue)) {
    return resolveFromEnv();
  }
  return resolveFromList(userValue);
}

// `userValue` is the value of `granularities` from the cube.js / cube.py config file.
//   undefined     -> fall back to `CUBEJS_GRANULARITIES` env vars
//   GranularityList  -> use this list, replacing env vars entirely (no merge)
//   function(ctx) -> called per request; same no-merge replacement as the list form
export async function resolveGlobalGranularities(
  userValue: GranularitiesOption,
  ctx: any,
): Promise<GlobalGranularitiesConfig> {
  if (typeof userValue === 'function') {
    const resolved = await userValue(ctx);
    return resolveGlobalGranularitiesSync(Array.isArray(resolved) ? resolved : undefined);
  }
  return resolveGlobalGranularitiesSync(userValue);
}

export function getBuiltInGranularityDefaults(name: string) {
  return BUILT_IN_GRANULARITIES[name];
}

// Resolve title/format/interval for each enabled built-in, applying any override from
// `globalConfig.customGranularities` (e.g. `{ name: 'year', title: 'Jaar' }` localizes the title).
// `interval` is filled in as "1 <name>" so built-ins share the same response shape as customs.
export type BuiltInCatalogEntry = {
  title: string;
  format: string;
  interval: string;
};

export function buildBuiltInsCatalog(globalConfig: GlobalGranularitiesConfig): Record<string, BuiltInCatalogEntry> {
  const catalog: Record<string, BuiltInCatalogEntry> = {};
  for (const name of globalConfig.enabledBuiltIns) {
    const defaults = BUILT_IN_GRANULARITIES[name];
    if (defaults) {
      const override = globalConfig.customGranularities[name];
      catalog[name] = {
        title: override?.title || defaults.title,
        format: override?.format || defaults.format,
        // Built-in interval is fixed at `1 <name>` — the SQL layer always buckets predefined
        // granularities that way, so an override interval must NOT be advertised (it would lie
        // about how the data is bucketed). Title/format overrides are display-only and safe.
        interval: `1 ${name}`,
      };
    }
  }
  return catalog;
}

// Canonical sha256 of a resolved config: equal hash iff identical effective sets. Order-sensitive
// on purpose — built-in and custom order affect the emitted meta. Used as the meta-variant cache
// key and compilerId discriminator. Values are already sanitized to strings at resolution time,
// so the projected fields here are exactly what the serializer emits — the hash can never
// disagree with the wire output (which would let two different tenant configs share one variant).
export function granularityConfigHash(config: GlobalGranularitiesConfig): string {
  const canonical = {
    builtIns: [...config.enabledBuiltIns],
    custom: Object.entries(config.customGranularities).map(([name, def]) => ({
      name,
      title: def.title,
      format: def.format,
      interval: def.interval,
      offset: def.offset,
      origin: def.origin,
    })),
  };
  return crypto.createHash('sha256').update(JSON.stringify(canonical)).digest('hex');
}
