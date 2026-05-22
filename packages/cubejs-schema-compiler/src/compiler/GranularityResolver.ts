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
    if ('includes' in raw || 'excludes' in raw || 'custom' in raw) {
      // New dict form. YamlCompiler has already keyed `custom` by name.
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
      const passesIncludes = includesAllowsAll || includesSet!.has(name);
      const blockedByExcludes = excludesSet!.has(name);
      if (passesIncludes && !blockedByExcludes) {
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
