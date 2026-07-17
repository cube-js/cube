import type { GranularityDefinition } from './CubeSymbols';

// `'*'` is the wildcard form of an includes/excludes list.
export type GranularityInclusionList = '*' | string[];

// Canonical form produced by `normalizeGranularitiesBlock`. All downstream readers
// (resolver, meta transformer, pre-agg matcher) only see this shape.
export type NormalizedGranularitiesBlock = {
  includes: GranularityInclusionList;
  excludes: GranularityInclusionList;
  custom: Record<string, GranularityDefinition>;
};

// Effective granularity set for one time dimension, ready to serialize into /v1/meta.
export type ResolvedGranularitySet = Record<string, GranularityDefinition & { type: 'built-in' | 'custom' }>;

// Used when a time dimension has no `granularities` block at all: take all enabled globals, no local customs.
const EMPTY_BLOCK: NormalizedGranularitiesBlock = {
  includes: '*',
  excludes: [],
  custom: {},
};

// Map any accepted input shape (omitted, legacy array, post-yaml keyed object, new dict)
// onto NormalizedGranularitiesBlock. Validator runs first, so malformed input never reaches here.
export function normalizeGranularitiesBlock(raw: any): NormalizedGranularitiesBlock {
  if (raw == null) {
    return EMPTY_BLOCK;
  }

  if (Array.isArray(raw)) {
    // Legacy form. Every array entry is a custom granularity; built-ins are inherited from globals.
    return {
      includes: '*',
      excludes: [],
      custom: Object.fromEntries(raw.filter(g => g && g.name).map(g => {
        const { name, ...rest } = g;
        return [name, rest];
      })),
    };
  }

  if (typeof raw === 'object') {
    // New dict form iff every key is one of includes/excludes/custom AND the values have the dict
    // shape (includes/excludes are '*' or arrays, custom is a plain object). The value check keeps a
    // legacy custom granularity named `includes`/`excludes`/`custom` — whose value is a granularity
    // definition object — from being misread as the dict form.
    const keys = Object.keys(raw);
    const isInclusionList = (v: any) => v === undefined || v === '*' || Array.isArray(v);
    const isCustomMap = (v: any) => v === undefined || (typeof v === 'object' && v !== null && !Array.isArray(v));
    const isDictForm =
      keys.length > 0 &&
      keys.every(k => k === 'includes' || k === 'excludes' || k === 'custom') &&
      isInclusionList(raw.includes) &&
      isInclusionList(raw.excludes) &&
      isCustomMap(raw.custom);
    if (isDictForm) {
      // YamlCompiler has already keyed `custom` by name.
      return {
        includes: raw.includes ?? '*',
        excludes: raw.excludes ?? [],
        custom: raw.custom ?? {},
      };
    }
    // Already-keyed legacy form (e.g. coming from JS configs, not YAML). Treat as custom-only.
    return {
      includes: '*',
      excludes: [],
      custom: raw,
    };
  }

  return EMPTY_BLOCK;
}

// The string-valued fields of a granularity definition, in wire-emission order. Single source of
// truth for every place that projects a definition (serialize, hash, sanitize, meta) so those
// projections can't drift — notably the hash and the serializer, which MUST agree or two tenant
// configs could collide on one variant-cache key.
export const GRANULARITY_STRING_FIELDS = ['title', 'interval', 'offset', 'origin', 'format'] as const;

// Wire shape of one entry in a time dimension's `effectiveGranularities`.
export type EffectiveGranularity = {
  name: string;
  type: 'built-in' | 'custom';
  title: string;
  interval?: string;
  offset?: string;
  origin?: string;
  format?: string;
};

// Serialize a resolved set for /v1/meta. `title` always present (falls back to the name); the
// other string fields are included only when defined. Order follows GRANULARITY_STRING_FIELDS,
// which also drives the config hash — the two share the field list so they can't drift.
export function serializeEffectiveGranularities(resolved: ResolvedGranularitySet): EffectiveGranularity[] {
  return Object.entries(resolved).map(([name, def]) => {
    const out: EffectiveGranularity = { name, type: def.type, title: def.title || name };
    for (const field of GRANULARITY_STRING_FIELDS) {
      if (field !== 'title' && def[field] !== undefined) {
        out[field] = def[field];
      }
    }
    return out;
  });
}

// Reconcile a dimension's local block against the global enabled built-ins and global customs,
// producing the effective set. Local customs always survive — even if local excludes is '*'.
export function resolveDimensionGranularities(
  localBlock: NormalizedGranularitiesBlock,
  globalEnabledBuiltIns: ReadonlyArray<string>,
  globalCustom: Readonly<Record<string, GranularityDefinition>>,
  allBuiltInsCatalog: Readonly<Record<string, GranularityDefinition>>,
): ResolvedGranularitySet {
  const out: ResolvedGranularitySet = {};

  const localIncludes = localBlock.includes;
  const localExcludes = localBlock.excludes;

  const includesAllowsAll = localIncludes === '*';
  const includesSet = includesAllowsAll ? null : new Set(localIncludes);
  const excludesBlocksAll = localExcludes === '*';
  const excludesSet = excludesBlocksAll ? null : new Set(localExcludes);

  // Built-ins and global customs are filtered the same way: keep iff included AND not excluded.
  if (!excludesBlocksAll) {
    for (const builtInName of globalEnabledBuiltIns) {
      const passesIncludes = includesAllowsAll || includesSet!.has(builtInName);
      const blockedByExcludes = excludesSet!.has(builtInName);
      const def = allBuiltInsCatalog[builtInName];
      if (passesIncludes && !blockedByExcludes && def) {
        out[builtInName] = { ...def, type: 'built-in' };
      }
    }
    for (const [name, def] of Object.entries(globalCustom)) {
      // A name shadowing a built-in is an override, already emitted as `type: 'built-in'` above with
      // its title/format folded in via `allBuiltInsCatalog`; skip it here so it isn't relabeled custom.
      const shadowsBuiltIn = !!allBuiltInsCatalog[name];
      const passesIncludes = includesAllowsAll || includesSet!.has(name);
      const blockedByExcludes = excludesSet!.has(name);
      if (!shadowsBuiltIn && passesIncludes && !blockedByExcludes) {
        out[name] = { ...def, type: 'custom' };
      }
    }
  }

  // Local customs are always emitted, even when `excludes: '*'` strips everything else.
  // Same-named local entry replaces a global one (last-write-wins).
  for (const [name, def] of Object.entries(localBlock.custom)) {
    out[name] = { ...def, type: 'custom' };
  }

  return out;
}

// One-shot: reconcile a dimension's block against the global config and serialize to the wire set.
// The single seam every path uses to turn a (block, config) into `effectiveGranularities`, so the
// resolve→serialize contract lives in exactly one place. A missing block means "no local block".
export function effectiveGranularitiesFor(
  block: NormalizedGranularitiesBlock | undefined,
  globalEnabledBuiltIns: ReadonlyArray<string>,
  globalCustom: Readonly<Record<string, GranularityDefinition>>,
  allBuiltInsCatalog: Readonly<Record<string, GranularityDefinition>>,
): EffectiveGranularity[] {
  return serializeEffectiveGranularities(resolveDimensionGranularities(
    block ?? normalizeGranularitiesBlock(undefined),
    globalEnabledBuiltIns,
    globalCustom,
    allBuiltInsCatalog,
  ));
}
