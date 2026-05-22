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
  return name in BUILT_IN_GRANULARITIES;
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
      } else {
        // Non-built-in name: pull the definition from `CUBEJS_GRANULARITIES_<NAME>_*` env vars.
        customGranularities[trimmed] = applyEnvOverrides(trimmed);
      }
    }
  }
  return { enabledBuiltIns, customGranularities };
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
      if (isBuiltInGranularity(name)) {
        // `{ name: 'year', title: 'Anno' }` both enables 'year' and overrides its title/format.
        enabledBuiltIns.push(name);
        customGranularities[name] = def;
      } else {
        customGranularities[name] = def;
      }
    }
  }
  return { enabledBuiltIns, customGranularities };
}

// `userValue` is the value of `granularities` from the cube.js / cube.py config file.
//   undefined     -> fall back to `CUBEJS_GRANULARITIES` env vars
//   GranularityList  -> use this list, replacing env vars entirely (no merge)
//   function(ctx) -> called per request; same no-merge replacement as the list form
export async function resolveGlobalGranularities(
  userValue: GranularityList | ((ctx: any) => GranularityList | Promise<GranularityList>) | undefined,
  ctx: any,
): Promise<GlobalGranularitiesConfig> {
  if (userValue === undefined) {
    return resolveFromEnv();
  }
  const resolved = typeof userValue === 'function' ? await userValue(ctx) : userValue;
  if (!Array.isArray(resolved)) {
    return resolveFromEnv();
  }
  return resolveFromList(resolved);
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
        interval: override?.interval || `1 ${name}`,
      };
    }
  }
  return catalog;
}
